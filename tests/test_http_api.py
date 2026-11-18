from __future__ import annotations

import asyncio
import hashlib
import json
import os
import time
from collections.abc import AsyncGenerator
from datetime import timedelta
from pathlib import Path
from typing import Any, cast

import pytest
from fastapi.testclient import TestClient
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.websockets import WebSocketDisconnect

from clio_relay.adapters.worker import door_errors as door_errors_module
from clio_relay.adapters.worker import http_api as http_api_module
from clio_relay.adapters.worker import http_api_streaming as http_api_streaming_module
from clio_relay.adapters.worker.core_queue import ClioCoreQueue
from clio_relay.adapters.worker.http_api import create_app
from clio_relay.adapters.worker.storage_runtime import StorageManagedQueue
from clio_relay.cluster_config import (
    CLUSTER_REGISTRY_ENV,
    ClusterDefinition,
    ClusterRegistry,
    cluster_route_revision,
)
from clio_relay.config import RelaySettings
from clio_relay.errors import ConfigurationError, NotFoundError
from clio_relay.job_identity import (
    OWNER_SESSION_ID_HEADER,
    SESSION_GENERATION_ID_HEADER,
)
from clio_relay.models import (
    ArtifactRef,
    Cursor,
    EndpointRegistration,
    EndpointRole,
    GatewaySession,
    GatewaySessionState,
    JarvisRunSpec,
    JobKind,
    JobState,
    JobWaitResult,
    MonitorRule,
    RelayJob,
    RelayTask,
    RemoteAgentTaskSpec,
    TaskTimelineEvent,
    utc_now,
)
from clio_relay.relay_ops import MAX_ARTIFACT_CONTENT_BYTES, job_wait_result
from clio_relay.session_api import session_identity_document


def _bind_owned_session_cluster_authority(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    *,
    definition: ClusterDefinition | None = None,
) -> ClusterDefinition:
    """Bind one exact test cluster definition as owned-session process authority."""
    bound_definition = definition or ClusterDefinition(
        name="test-cluster",
        ssh_host="test-cluster",
    )
    registry_path = tmp_path / "session-authority" / "clusters.json"
    ClusterRegistry(clusters={bound_definition.name: bound_definition}).save(registry_path)
    payload = registry_path.read_bytes()
    monkeypatch.setenv(CLUSTER_REGISTRY_ENV, str(registry_path))
    monkeypatch.setenv(
        "CLIO_RELAY_SESSION_REGISTRY_SHA256",
        hashlib.sha256(payload).hexdigest(),
    )
    monkeypatch.setenv(
        "CLIO_RELAY_SESSION_ROUTE_REVISION",
        cluster_route_revision(bound_definition),
    )
    return bound_definition


def _parse_sse_frames(body: str) -> list[dict[str, str]]:
    """Parse a raw ``text/event-stream`` body into ``{event, data, id}`` frames.

    A minimal test-only parser matching the wire shape
    :func:`clio_relay.adapters.worker.http_api_streaming._log_tail_sse_events` writes:
    each frame is one ``id:``/``event:``/``data:`` line group terminated by
    a blank line.
    """
    frames: list[dict[str, str]] = []
    event = "message"
    data_lines: list[str] = []
    frame_id = ""
    for raw_line in body.split("\n"):
        line = raw_line.rstrip("\r")
        if line == "":
            if data_lines:
                frames.append({"event": event, "data": "\n".join(data_lines), "id": frame_id})
            event = "message"
            data_lines = []
            frame_id = ""
            continue
        if line.startswith("data:"):
            data_lines.append(line[len("data:") :].removeprefix(" "))
        elif line.startswith("event:"):
            event = line[len("event:") :].removeprefix(" ")
        elif line.startswith("id:"):
            frame_id = line[len("id:") :].removeprefix(" ")
    return frames


def test_http_monitor_logs_and_artifact_content(tmp_path: Path) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_yaml="name: generic\npkgs: []\n"),
            idempotency_key="http",
        )
    )
    spool = settings.spool_dir / job.job_id
    spool.mkdir(parents=True)
    stdout_path = spool / "stdout.log"
    stdout_path.write_text("hello from http\n", encoding="utf-8")
    artifact = queue.append_artifact(
        ArtifactRef(job_id=job.job_id, uri=stdout_path.as_uri(), kind="stdout")
    )
    client = cast(Any, TestClient(create_app(settings)))

    monitor_response = client.get(f"/jobs/{job.job_id}/monitor")
    log_response = client.get(f"/jobs/{job.job_id}/logs/stdout", params={"limit": 5})
    artifact_response = client.get(f"/artifacts/{artifact.artifact_id}/content")
    invalid_log_responses = [
        client.get(f"/jobs/{job.job_id}/logs/stdout", params={"offset": -1}),
        client.get(f"/jobs/{job.job_id}/logs/stdout", params={"limit": 0}),
        client.get(f"/jobs/{job.job_id}/logs/stdout", params={"limit": 1_048_577}),
    ]

    assert monitor_response.status_code == 200
    assert monitor_response.json()["job"]["job_id"] == job.job_id
    assert log_response.status_code == 200
    assert log_response.json()["text"] == "hello"
    assert artifact_response.status_code == 200
    assert artifact_response.json()["artifact"]["artifact_id"] == artifact.artifact_id
    assert [response.status_code for response in invalid_log_responses] == [422, 422, 422]


def test_get_log_serves_console_stream_like_stdout_and_stderr(tmp_path: Path) -> None:
    """#259: the door route accepts and serves ``console`` exactly like the
    other two log streams -- same offset/limit/eof envelope shape, and a
    stream nothing has written yet reads back empty with ``eof`` rather than
    404ing or 500ing (point 2c: jobs that never write console must never
    fail the read)."""
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_yaml="name: generic\npkgs: []\n"),
            idempotency_key="http-console",
        )
    )
    spool = settings.spool_dir / job.job_id
    spool.mkdir(parents=True)
    client = cast(Any, TestClient(create_app(settings)))

    empty_response = client.get(f"/jobs/{job.job_id}/logs/console")
    assert empty_response.status_code == 200
    assert empty_response.json() == {
        "job_id": job.job_id,
        "stream": "console",
        "offset": 0,
        "next_offset": 0,
        "eof": True,
        "text": "",
    }

    console_path = spool / "console.log"
    console_path.write_bytes(b"thermo: step 0 temp 300.0\n")

    log_response = client.get(f"/jobs/{job.job_id}/logs/console", params={"limit": 6})
    invalid_stream_response = client.get(f"/jobs/{job.job_id}/logs/bogus-stream")

    assert log_response.status_code == 200
    body = log_response.json()
    assert body["stream"] == "console"
    assert body["text"] == "thermo"
    assert body["eof"] is False
    assert body["next_offset"] == 6

    tail_response = client.get(
        f"/jobs/{job.job_id}/logs/console",
        params={"offset": body["next_offset"]},
    )
    assert tail_response.status_code == 200
    assert tail_response.json()["text"] == ": step 0 temp 300.0\n"
    assert tail_response.json()["eof"] is True

    assert invalid_stream_response.status_code == 400
    assert invalid_stream_response.json()["reason"] == "log_stream_invalid"
    assert "console" in invalid_stream_response.json()["detail"]


def test_get_log_serves_console_stderr_stream_like_console(tmp_path: Path) -> None:
    """clio-relay#259 residual: the door route accepts and serves
    ``console_stderr`` -- the application's stderr channel, ``console``'s
    sibling -- with the exact same offset/limit/eof envelope shape, and the
    same empty-with-eof read for a stream nothing has written yet."""
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_yaml="name: generic\npkgs: []\n"),
            idempotency_key="http-console-stderr",
        )
    )
    spool = settings.spool_dir / job.job_id
    spool.mkdir(parents=True)
    client = cast(Any, TestClient(create_app(settings)))

    empty_response = client.get(f"/jobs/{job.job_id}/logs/console_stderr")
    assert empty_response.status_code == 200
    assert empty_response.json() == {
        "job_id": job.job_id,
        "stream": "console_stderr",
        "offset": 0,
        "next_offset": 0,
        "eof": True,
        "text": "",
    }

    console_stderr_path = spool / "console_stderr.log"
    console_stderr_path.write_bytes(b"WARNING: rank 3 near OOM\n")

    log_response = client.get(f"/jobs/{job.job_id}/logs/console_stderr", params={"limit": 7})

    assert log_response.status_code == 200
    body = log_response.json()
    assert body["stream"] == "console_stderr"
    assert body["text"] == "WARNING"
    assert body["eof"] is False
    assert body["next_offset"] == 7

    tail_response = client.get(
        f"/jobs/{job.job_id}/logs/console_stderr",
        params={"offset": body["next_offset"]},
    )
    assert tail_response.status_code == 200
    assert tail_response.json()["text"] == ": rank 3 near OOM\n"
    assert tail_response.json()["eof"] is True


def test_http_log_sse_streams_chunk_and_ends_on_terminal_eof(tmp_path: Path) -> None:
    """clio-relay#221/#259: the SSE log-tail route's basic shape -- a
    ``log_chunk`` event carrying the already-written bytes, then a typed
    ``end`` event once the owning job is terminal AND the stream is
    drained, on the SAME auth/stream vocabulary as the byte-range route
    right above it."""
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_yaml="name: generic\npkgs: []\n"),
            idempotency_key="http-log-sse",
        )
    )
    spool = settings.spool_dir / job.job_id
    spool.mkdir(parents=True)
    (spool / "console.log").write_bytes(b"thermo: step 0 temp 300.0\n")
    queue.update_job_state(job.job_id, JobState.SUCCEEDED)
    client = cast(Any, TestClient(create_app(settings)))

    with client.stream(
        "GET",
        f"/jobs/{job.job_id}/logs/console/sse",
        params={"poll_seconds": 0.05},
    ) as response:
        body = response.read().decode("utf-8")

    written_bytes = len(b"thermo: step 0 temp 300.0\n")
    frames = _parse_sse_frames(body)
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/event-stream")
    log_chunk_frames = [frame for frame in frames if frame["event"] == "log_chunk"]
    end_frames = [frame for frame in frames if frame["event"] == "end"]
    assert len(log_chunk_frames) == 1
    assert len(end_frames) == 1
    chunk_frame = log_chunk_frames[0]
    end_frame = end_frames[0]
    chunk_data = json.loads(chunk_frame["data"])
    assert chunk_data == {
        "job_id": job.job_id,
        "stream": "console",
        "chunk": "thermo: step 0 temp 300.0\n",
        "offset": 0,
        "next_offset": written_bytes,
    }
    # The `id:` line on the log_chunk frame carries the resumable offset.
    assert chunk_frame["id"] == str(written_bytes)
    end_data = json.loads(end_frame["data"])
    assert end_data == {
        "job_id": job.job_id,
        "stream": "console",
        "state": "succeeded",
        "offset": written_bytes,
        "next_offset": written_bytes,
    }
    assert frames.index(chunk_frame) < frames.index(end_frame)


def test_http_log_sse_resumes_from_offset(tmp_path: Path) -> None:
    """clio-relay#221/#259: a client reconnecting with a later ``offset``
    (after a drop, or simply resuming) only receives the remainder -- same
    byte-offset semantics as the byte-range route's own resume behavior."""
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_yaml="name: generic\npkgs: []\n"),
            idempotency_key="http-log-sse-resume",
        )
    )
    spool = settings.spool_dir / job.job_id
    spool.mkdir(parents=True)
    (spool / "console.log").write_bytes(b"thermo: step 0\nthermo: step 1\n")
    queue.update_job_state(job.job_id, JobState.SUCCEEDED)
    client = cast(Any, TestClient(create_app(settings)))

    with client.stream(
        "GET",
        f"/jobs/{job.job_id}/logs/console/sse",
        params={"offset": len("thermo: step 0\n"), "poll_seconds": 0.05},
    ) as response:
        body = response.read().decode("utf-8")

    assert "thermo: step 0" not in body
    assert "thermo: step 1" in body
    assert "event: end" in body


def test_http_log_sse_typed_404_for_missing_job_and_400_for_bad_stream(tmp_path: Path) -> None:
    """clio-relay#221/#259: nonexistent job -> ``job_not_found`` (404), same
    reason the byte-range route raises; unknown stream name ->
    ``log_stream_invalid`` (400), same reason and status too."""
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_yaml="name: generic\npkgs: []\n"),
            idempotency_key="http-log-sse-404",
        )
    )
    client = cast(Any, TestClient(create_app(settings)))

    missing_job = client.get("/jobs/does-not-exist/logs/console/sse")
    bad_stream = client.get(f"/jobs/{job.job_id}/logs/bogus-stream/sse")

    assert missing_job.status_code == 404
    assert missing_job.json()["reason"] == "job_not_found"
    assert bad_stream.status_code == 400
    assert bad_stream.json()["reason"] == "log_stream_invalid"


def test_http_log_sse_enforces_configured_token(tmp_path: Path) -> None:
    """clio-relay#221/#259: the SSE route shares the byte-range route's exact
    ``auth_dependency`` -- proven directly here rather than only inferred
    from the general app-wide auth test."""
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
        api_token="secret-token",
    )
    queue = ClioCoreQueue(settings.core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_yaml="name: generic\npkgs: []\n"),
            idempotency_key="http-log-sse-auth",
        )
    )
    spool = settings.spool_dir / job.job_id
    spool.mkdir(parents=True)
    queue.update_job_state(job.job_id, JobState.SUCCEEDED)
    client = cast(Any, TestClient(create_app(settings)))
    path = f"/jobs/{job.job_id}/logs/console/sse"

    missing = client.get(path)
    with client.stream("GET", path, headers={"Authorization": "Bearer secret-token"}) as authorized:
        authorized.read()

    assert missing.status_code == 401
    assert authorized.status_code == 200


@pytest.mark.asyncio
async def test_log_tail_sse_events_pushes_appended_chunk_within_follow_interval(
    tmp_path: Path,
) -> None:
    """clio-relay#221/#259: the core latency proof, at the unit the design
    calls out -- the follow LOOP, not the whole HTTP stack. Drives
    ``_log_tail_sse_events`` directly on a real ``asyncio`` event loop: a
    sibling task writes the append only after the generator has entered its
    first (no-data-yet) ``asyncio.sleep`` wait, so the chunk is genuinely
    picked up mid-wait, not merely caught up on connect. Timed with
    ``asyncio.wait_for`` against real wall-clock elapsed time -- never a
    sleep-then-check race.

    Adversarial review (review item 9): drives the route's DEFAULT
    ``poll_seconds`` (never a hand-picked fast literal) and asserts the
    constant's own value directly, so a regression toward the old 2.0s
    capture-side floor fails HERE, at the source, not only via a generous
    downstream elapsed bound. ``wait_for``'s own timeout is comfortably
    ABOVE the elapsed assertion's bound, so a slow-but-passing run cannot be
    misreported as a timeout instead of a bound failure.
    """
    assert http_api_streaming_module.LOG_SSE_FOLLOW_POLL_SECONDS <= 0.5, (
        "the default follow interval regressed toward the old capture-side "
        "floor -- this would blow the live-console lane's <=2s acceptance budget"
    )
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_yaml="name: generic\npkgs: []\n"),
            idempotency_key="log-sse-latency",
        )
    )
    queue.update_job_state(job.job_id, JobState.RUNNING)
    spool = settings.spool_dir / job.job_id
    spool.mkdir(parents=True)
    console_path = spool / "console.log"
    console_path.write_bytes(b"")

    async def write_after_first_poll() -> None:
        # One loop tick is enough: `_log_tail_sse_events`'s first iteration
        # runs its (synchronous) empty-file read to completion and reaches
        # its own `await asyncio.sleep(poll_seconds)` before this resumes,
        # so the write below lands strictly DURING that wait, never before it.
        await asyncio.sleep(0)
        console_path.write_bytes(b"thermo: step 0 temp 300.0\n")

    generator = http_api_streaming_module._log_tail_sse_events(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        settings,
        queue,
        job.job_id,
        stream_name="console",
        offset=0,
        poll_seconds=http_api_streaming_module.LOG_SSE_FOLLOW_POLL_SECONDS,
    )
    writer = asyncio.ensure_future(write_after_first_poll())
    start = time.monotonic()
    try:
        first_frame = await asyncio.wait_for(generator.__anext__(), timeout=2.0)
    finally:
        await writer
    elapsed = time.monotonic() - start

    assert elapsed < 1.0, f"appended chunk arrived after {elapsed:.3f}s"
    assert "event: log_chunk" in first_frame
    assert "thermo: step 0 temp 300.0" in first_frame
    await cast(AsyncGenerator[str, None], generator).aclose()


@pytest.mark.asyncio
async def test_log_tail_sse_events_emits_keepalive_within_interval_while_idle(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """clio-relay#221/#259 adversarial review (D1): a job that is RUNNING but
    quiet (no new bytes) emits an SSE comment keepalive at least every
    ``LOG_SSE_KEEPALIVE_INTERVAL_SECONDS``, so a client's own socket read
    never times out against a merely-quiet job. The interval is
    monkeypatched tiny so this stays a fast unit test -- never a real 10s
    wait -- and the keepalive line is proven to never dispatch as a frame
    (the client's own SSE parser already treats ``:``-prefixed lines as
    comments, never a field)."""
    monkeypatch.setattr(http_api_streaming_module, "LOG_SSE_KEEPALIVE_INTERVAL_SECONDS", 0.05)
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_yaml="name: generic\npkgs: []\n"),
            idempotency_key="log-sse-keepalive",
        )
    )
    queue.update_job_state(job.job_id, JobState.RUNNING)
    spool = settings.spool_dir / job.job_id
    spool.mkdir(parents=True)

    generator = http_api_streaming_module._log_tail_sse_events(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        settings,
        queue,
        job.job_id,
        stream_name="console",
        offset=0,
        poll_seconds=0.02,
    )
    try:
        frame = await asyncio.wait_for(generator.__anext__(), timeout=2.0)
    finally:
        await cast(AsyncGenerator[str, None], generator).aclose()

    assert frame == ": keepalive\n\n"


@pytest.mark.asyncio
async def test_log_tail_sse_events_treats_vanished_job_as_typed_end_state_gone(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """clio-relay#221/#259 adversarial review (D7): a job record that
    vanishes mid-stream (``queue.get_job`` raising ``NotFoundError`` on a
    LATER poll, after the route's own upfront admission check already
    passed) closes with a typed ``end`` carrying ``state: "gone"`` instead
    of leaking an unhandled 500 after headers were already sent -- which a
    client would see as a truncated 200, misread as a channel drop."""
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_yaml="name: generic\npkgs: []\n"),
            idempotency_key="log-sse-vanish",
        )
    )
    spool = settings.spool_dir / job.job_id
    spool.mkdir(parents=True)

    def vanished(_job_id: str) -> RelayJob:
        raise NotFoundError(f"job vanished mid-stream: {_job_id}")

    monkeypatch.setattr(queue, "get_job", vanished)

    generator = http_api_streaming_module._log_tail_sse_events(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        settings,
        queue,
        job.job_id,
        stream_name="console",
        offset=0,
        poll_seconds=0.05,
    )
    frame = await asyncio.wait_for(generator.__anext__(), timeout=2.0)

    assert "event: end" in frame
    end_data = json.loads(frame.splitlines()[1].removeprefix("data: "))
    assert end_data == {
        "job_id": job.job_id,
        "stream": "console",
        "state": "gone",
        "offset": 0,
        "next_offset": 0,
    }
    with pytest.raises(StopAsyncIteration):
        await generator.__anext__()


def _owned_session_settings(tmp_path: Path) -> RelaySettings:
    return RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
        owner_session_id="desktop-session-1",
        owner_session_generation_id="generation-1",
        owner_session_cluster="test-cluster",
        api_token="session-api-token",
        session_owner_token="o" * 32,
    )


@pytest.mark.asyncio
async def test_log_tail_sse_events_renews_the_owned_session_lease_on_each_poll_tick(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """iowarp/clio-relay#277 adversarial-review HIGH 4: the auth dependency
    that would otherwise renew the lease evaluates only ONCE per stream --
    a console session followed longer than the lease TTL would otherwise be
    reaped mid-stream. The "vanished job" shape bounds this to one poll
    tick deterministically (real timing, no sleep-based race)."""
    settings = _owned_session_settings(tmp_path)
    queue = ClioCoreQueue(settings.core_dir)

    def vanished(_job_id: str) -> RelayJob:
        raise NotFoundError(f"job vanished mid-stream: {_job_id}")

    monkeypatch.setattr(queue, "get_job", vanished)
    generator = http_api_streaming_module._log_tail_sse_events(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        settings,
        queue,
        "job-does-not-matter",
        stream_name="console",
        offset=0,
        poll_seconds=0.05,
    )
    await asyncio.wait_for(generator.__anext__(), timeout=2.0)

    lease = queue.owner_session_lease_status(
        "desktop-session-1", session_generation_id="generation-1"
    )
    assert lease is not None
    assert lease.status == "open"


@pytest.mark.asyncio
async def test_task_stream_payloads_renews_the_owned_session_lease(tmp_path: Path) -> None:
    """Renewal is the FIRST statement of the loop, before ``drain_task_events``
    -- proven by it having already happened even though a nonexistent task
    id makes that very next line raise ``NotFoundError``."""
    settings = _owned_session_settings(tmp_path)
    queue = ClioCoreQueue(settings.core_dir)

    with pytest.raises(NotFoundError):
        async for _payload in http_api_streaming_module._task_stream_payloads(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            queue,
            "task-does-not-matter",
            cursor=1,
            limit=100,
            poll_seconds=0.05,
            stop_after_replay=True,
            settings=settings,
        ):
            pass

    lease = queue.owner_session_lease_status(
        "desktop-session-1", session_generation_id="generation-1"
    )
    assert lease is not None
    assert lease.status == "open"


@pytest.mark.asyncio
async def test_monitor_stream_payloads_renews_the_owned_session_lease(tmp_path: Path) -> None:
    settings = _owned_session_settings(tmp_path)
    queue = ClioCoreQueue(settings.core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_yaml="name: generic\npkgs: []\n"),
            idempotency_key="monitor-lease-renewal",
        )
    )
    queue.update_job_state(job.job_id, JobState.SUCCEEDED)

    payloads = [
        payload
        async for payload in http_api_streaming_module._monitor_stream_payloads(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            queue,
            job.job_id,
            cursor=1,
            limit=100,
            poll_seconds=0.05,
            stop_on_terminal=True,
            settings=settings,
        )
    ]

    assert any(payload.get("event") == "terminal" for payload in payloads)
    lease = queue.owner_session_lease_status(
        "desktop-session-1", session_generation_id="generation-1"
    )
    assert lease is not None
    assert lease.status == "open"


@pytest.mark.asyncio
async def test_stream_payload_generators_skip_renewal_when_settings_is_none(
    tmp_path: Path,
) -> None:
    """Purely additive: every pre-existing caller that does not pass
    ``settings`` (an unowned/local API, or a test that predates #277) must
    see zero behavior change -- no lease is ever created."""
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)

    with pytest.raises(NotFoundError):
        async for _payload in http_api_streaming_module._task_stream_payloads(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            queue,
            "task-does-not-matter",
            cursor=1,
            limit=100,
            poll_seconds=0.05,
            stop_after_replay=True,
        ):
            pass

    assert queue.owner_session_lease_status("desktop-session-1") is None


@pytest.mark.asyncio
async def test_log_tail_sse_events_decodes_utf8_character_straddling_read_boundary(
    tmp_path: Path,
) -> None:
    """clio-relay#221/#259 adversarial review (D9), the reviewer's own live
    probe reproduced as a fast unit test: a multi-byte UTF-8 character
    written exactly straddling a 1 MiB read-chunk boundary must decode
    correctly across two reads, never as two U+FFFD replacement
    characters. Proven by holding one incremental decoder across both
    ``__anext__()`` calls, exactly as the generator itself does."""
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_yaml="name: generic\npkgs: []\n"),
            idempotency_key="log-sse-utf8-boundary",
        )
    )
    spool = settings.spool_dir / job.job_id
    spool.mkdir(parents=True)
    # "é" encodes to two UTF-8 bytes (0xC3 0xA9). Padding it to exactly
    # straddle the 1 MiB (MAX_LOG_READ_BYTES) read boundary means the FIRST
    # read ends with only the first byte of "é", and the SECOND read starts
    # with its remaining byte.
    max_read_bytes = http_api_streaming_module.MAX_LOG_READ_BYTES
    padding = "a" * (max_read_bytes - 1)
    multibyte_character = "é"
    (spool / "console.log").write_bytes(
        (padding + multibyte_character).encode("utf-8") + b"\ntail\n"
    )
    queue.update_job_state(job.job_id, JobState.SUCCEEDED)

    generator = http_api_streaming_module._log_tail_sse_events(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        settings,
        queue,
        job.job_id,
        stream_name="console",
        offset=0,
        poll_seconds=0.05,
    )
    collected_text = ""
    try:
        async for frame in generator:
            if "event: log_chunk" not in frame:
                continue
            chunk_data = json.loads(frame.splitlines()[2].removeprefix("data: "))
            collected_text += chunk_data["chunk"]
    finally:
        await cast(AsyncGenerator[str, None], generator).aclose()

    assert "�" not in collected_text
    assert collected_text == padding + multibyte_character + "\ntail\n"


def test_http_log_sse_prefers_last_event_id_over_offset_query_param(tmp_path: Path) -> None:
    """clio-relay#221/#259 adversarial review (D3): a spec-compliant
    ``EventSource`` reconnects with its own ``Last-Event-ID`` header, not a
    caller-managed ``?offset=`` -- previously accepted and silently
    ignored (restarting at 0, re-delivering already-seen bytes). When
    present and a parseable non-negative integer, it is now preferred."""
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_yaml="name: generic\npkgs: []\n"),
            idempotency_key="log-sse-last-event-id",
        )
    )
    spool = settings.spool_dir / job.job_id
    spool.mkdir(parents=True)
    (spool / "console.log").write_bytes(b"thermo: step 0\nthermo: step 1\n")
    queue.update_job_state(job.job_id, JobState.SUCCEEDED)
    client = cast(Any, TestClient(create_app(settings)))

    with client.stream(
        "GET",
        f"/jobs/{job.job_id}/logs/console/sse",
        params={"offset": 0, "poll_seconds": 0.05},
        headers={"Last-Event-ID": str(len(b"thermo: step 0\n"))},
    ) as response:
        body = response.read().decode("utf-8")

    assert "thermo: step 0" not in body
    assert "thermo: step 1" in body


def test_http_log_sse_rejects_garbage_last_event_id(tmp_path: Path) -> None:
    """clio-relay#221/#259 adversarial review (D3): an unparseable or
    negative ``Last-Event-ID`` is a typed 400, never silently ignored or
    treated as offset 0."""
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_yaml="name: generic\npkgs: []\n"),
            idempotency_key="log-sse-bad-last-event-id",
        )
    )
    client = cast(Any, TestClient(create_app(settings)))

    garbage = client.get(
        f"/jobs/{job.job_id}/logs/console/sse", headers={"Last-Event-ID": "not-a-number"}
    )
    negative = client.get(f"/jobs/{job.job_id}/logs/console/sse", headers={"Last-Event-ID": "-1"})

    for response in (garbage, negative):
        assert response.status_code == 400
        assert response.json()["reason"] == "log_offset_invalid"


def test_http_log_sse_rejects_offset_past_current_eof(tmp_path: Path) -> None:
    """clio-relay#221/#259 adversarial review (D5): an offset already past
    the stream's CURRENT size at request time is a typed refusal, not a
    silently-affirmed clean ``end`` as if the offset had ever been valid."""
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_yaml="name: generic\npkgs: []\n"),
            idempotency_key="log-sse-beyond-eof",
        )
    )
    spool = settings.spool_dir / job.job_id
    spool.mkdir(parents=True)
    (spool / "console.log").write_bytes(b"only ten\n")
    client = cast(Any, TestClient(create_app(settings)))

    response = client.get(f"/jobs/{job.job_id}/logs/console/sse", params={"offset": 10_000})

    assert response.status_code == 400
    assert response.json()["reason"] == "log_offset_beyond_eof"


def test_http_log_sse_rejects_non_finite_and_oversized_poll_seconds(tmp_path: Path) -> None:
    """clio-relay#221/#259 adversarial review (D6): ``poll_seconds`` is
    guarded against nan/inf and pathologically large values, the same
    shape ``RemoteConnection.request_json`` already uses for its own
    timeout guard -- never a wedged follow loop."""
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_yaml="name: generic\npkgs: []\n"),
            idempotency_key="log-sse-poll-guard",
        )
    )
    client = cast(Any, TestClient(create_app(settings)))

    responses = [
        client.get(f"/jobs/{job.job_id}/logs/console/sse", params={"poll_seconds": "nan"}),
        client.get(f"/jobs/{job.job_id}/logs/console/sse", params={"poll_seconds": "inf"}),
        client.get(f"/jobs/{job.job_id}/logs/console/sse", params={"poll_seconds": 999}),
        client.get(f"/jobs/{job.job_id}/logs/console/sse", params={"poll_seconds": 0}),
    ]

    for response in responses:
        assert response.status_code == 400, response.text
        assert response.json()["reason"] == "poll_interval_invalid"


def test_oversized_artifact_content_answers_413_payload_too_large_not_200(
    tmp_path: Path,
) -> None:
    """F2 (#231 R6 review): GET /artifacts/{id}/content used to answer 200
    for an over-budget read, its body merely SAYING ``result_available:
    false`` -- a client checking only the status code would treat that as
    success. Routes through door_errors' existing ``payload_too_large``
    door (413) instead, the refusal document riding along as the envelope's
    extension data.
    """
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_yaml="name: generic\npkgs: []\n"),
            idempotency_key="http-oversized-artifact",
        )
    )
    spool = settings.spool_dir / job.job_id
    spool.mkdir(parents=True)
    oversized_path = spool / "large.bin"
    with oversized_path.open("wb") as stream:
        stream.truncate(MAX_ARTIFACT_CONTENT_BYTES + 1)
    artifact = queue.append_artifact(
        ArtifactRef(
            job_id=job.job_id,
            uri=oversized_path.as_uri(),
            kind="stdout",
            size_bytes=MAX_ARTIFACT_CONTENT_BYTES + 1,
        )
    )
    client = cast(Any, TestClient(create_app(settings)))

    response = client.get(f"/artifacts/{artifact.artifact_id}/content")

    assert response.status_code == 413
    document = response.json()
    assert document["schema_version"] == door_errors_module.SCHEMA_VERSION
    assert document["reason"] == "payload_too_large"
    assert document["status"] == 413
    assert document["result_available"] is False
    assert document["delivery"]["code"] == "artifact_content_too_large"
    assert document["artifact"]["artifact_id"] == artifact.artifact_id
    assert "data" not in document


def test_http_wait_returns_nonterminal_durable_state_when_observation_expires(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_name="long-http-run"),
            idempotency_key="long-http-run",
        )
    )
    observed_arguments: list[tuple[str, float, float]] = []

    def observe(
        selected_queue: ClioCoreQueue,
        job_id: str,
        *,
        timeout_seconds: float,
        poll_seconds: float,
    ) -> JobWaitResult:
        observed_arguments.append((job_id, timeout_seconds, poll_seconds))
        return job_wait_result(
            selected_queue.get_job(job_id),
            timeout_seconds=timeout_seconds,
        )

    # split/http-api-w3 (iowarp/clio-relay#231): `wait` moved to
    # http_api_routes_events.py, the only remaining caller of
    # observe_until_terminal -- the patch target follows the move.
    import clio_relay.adapters.worker.http_api_routes_events as http_api_routes_events_module

    monkeypatch.setattr(http_api_routes_events_module, "observe_until_terminal", observe)
    response = cast(Any, TestClient(create_app(settings))).post(
        f"/jobs/{job.job_id}/wait",
        params={"timeout_seconds": 0.25, "poll_seconds": 0.05},
    )

    assert response.status_code == 200
    assert response.json()["job_id"] == job.job_id
    assert response.json()["state"] == "queued"
    assert response.json()["observation"] == {
        "outcome": "observation_unknown",
        "timeout_seconds": 0.25,
        "scheduler_action": "none",
        "relay_action": "none",
    }
    assert observed_arguments == [(job.job_id, 0.25, 0.05)]
    assert queue.get_job(job.job_id).state is JobState.QUEUED


@pytest.mark.parametrize(
    ("field", "value"),
    [("timeout_seconds", "inf"), ("poll_seconds", "inf")],
)
def test_http_wait_rejects_nonfinite_observation_bounds(
    tmp_path: Path,
    field: str,
    value: str,
) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    response = cast(Any, TestClient(create_app(settings))).post(
        "/jobs/job_00000000000000000000000000000001/wait",
        params={field: value},
    )

    assert response.status_code == 422
    assert "positive and finite" in response.json()["detail"]


def test_http_monitor_sse_streams_monitor_and_terminal_events(tmp_path: Path) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_yaml="name: generic\npkgs: []\n"),
            idempotency_key="http-sse",
        )
    )
    queue.update_job_state(job.job_id, JobState.SUCCEEDED)
    client = cast(Any, TestClient(create_app(settings)))

    with client.stream("GET", f"/jobs/{job.job_id}/monitor/sse") as response:
        body = response.read().decode("utf-8")

    assert response.status_code == 200
    assert "event: monitor" in body
    assert "event: terminal" in body
    assert job.job_id in body


def test_http_monitor_websocket_streams_monitor_and_terminal_events(tmp_path: Path) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_yaml="name: generic\npkgs: []\n"),
            idempotency_key="http-websocket",
        )
    )
    queue.update_job_state(job.job_id, JobState.SUCCEEDED)
    client = cast(Any, TestClient(create_app(settings)))

    with client.websocket_connect(f"/jobs/{job.job_id}/monitor/ws") as websocket:
        monitor = websocket.receive_json()
        terminal = websocket.receive_json()

    assert monitor["event"] == "monitor"
    assert monitor["data"]["job"]["job_id"] == job.job_id
    assert terminal == {
        "event": "terminal",
        "data": {"job_id": job.job_id, "state": "succeeded"},
    }


def test_http_monitor_websocket_streams_running_then_terminal(tmp_path: Path) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_yaml="name: generic\npkgs: []\n"),
            idempotency_key="http-websocket-running",
        )
    )
    queue.update_job_state(job.job_id, JobState.RUNNING)
    client = cast(Any, TestClient(create_app(settings)))

    with client.websocket_connect(f"/jobs/{job.job_id}/monitor/ws?poll_seconds=0.01") as websocket:
        running = websocket.receive_json()
        queue.update_job_state(job.job_id, JobState.SUCCEEDED)
        messages: list[dict[str, Any]] = []
        for _ in range(10):
            message = cast(dict[str, Any], websocket.receive_json())
            messages.append(message)
            if message["event"] == "terminal":
                break

    assert running["event"] == "monitor"
    assert running["data"]["job"]["state"] == "running"
    assert {
        "event": "terminal",
        "data": {"job_id": job.job_id, "state": "succeeded"},
    } in messages


def test_http_event_and_monitor_limits_reject_huge_values_before_queue_reads(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_yaml="name: generic\npkgs: []\n"),
            idempotency_key="http-huge-page",
        )
    )
    task = queue.append_task(RelayTask(job_id=job.job_id, name="bounded.events"))
    client = cast(Any, TestClient(create_app(settings)))

    def unexpected_read(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("query validation must run before queue access")

    monkeypatch.setattr(ClioCoreQueue, "get_job", unexpected_read)
    monkeypatch.setattr(ClioCoreQueue, "get_task", unexpected_read)
    monkeypatch.setattr(ClioCoreQueue, "drain_events", unexpected_read)
    monkeypatch.setattr(ClioCoreQueue, "drain_task_events", unexpected_read)
    monkeypatch.setattr(ClioCoreQueue, "list_monitor_rules", unexpected_read)

    requests = [
        client.get(f"/jobs/{job.job_id}/events", params={"limit": 10**12}),
        client.get(f"/tasks/{task.task_id}/events", params={"limit": 10**12}),
        client.get(f"/tasks/{task.task_id}/events/sse", params={"limit": 10**12}),
        client.get(f"/jobs/{job.job_id}/monitor", params={"limit": 10**12}),
        client.get(f"/jobs/{job.job_id}/monitor/sse", params={"limit": 10**12}),
        client.post("/monitor/run-once", params={"limit": 10**12}),
    ]

    assert [response.status_code for response in requests] == [422] * len(requests)


def test_http_websocket_limits_reject_huge_values_before_accept_or_queue_reads(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_yaml="name: generic\npkgs: []\n"),
            idempotency_key="http-huge-websocket-page",
        )
    )
    task = queue.append_task(RelayTask(job_id=job.job_id, name="bounded.websocket"))
    client = cast(Any, TestClient(create_app(settings)))

    def unexpected_read(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("WebSocket validation must run before queue access")

    monkeypatch.setattr(ClioCoreQueue, "get_job", unexpected_read)
    monkeypatch.setattr(ClioCoreQueue, "get_task", unexpected_read)

    paths = [
        f"/jobs/{job.job_id}/monitor/ws?limit={10**12}",
        f"/tasks/{task.task_id}/events/ws?limit={10**12}",
    ]
    for path in paths:
        with pytest.raises(WebSocketDisconnect) as caught, client.websocket_connect(path):
            pass
        assert caught.value.code == 1008
        assert caught.value.reason == "websocket_page_limit_invalid"


def test_http_websocket_refusal_classes_have_registered_close_reasons(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plain_settings = RelaySettings(
        core_dir=tmp_path / "plain-core",
        spool_dir=tmp_path / "plain-spool",
    )
    plain_client = cast(Any, TestClient(create_app(plain_settings)))
    authenticated_client = cast(
        Any,
        TestClient(
            create_app(
                RelaySettings(
                    core_dir=tmp_path / "auth-core",
                    spool_dir=tmp_path / "auth-spool",
                    api_token="api-token",
                )
            )
        ),
    )

    _bind_owned_session_cluster_authority(monkeypatch, tmp_path)
    owned_settings = RelaySettings(
        core_dir=tmp_path / "owned-core",
        spool_dir=tmp_path / "owned-spool",
        api_token="api-token",
        owner_session_id="desktop-session-1",
        owner_session_generation_id="generation-1",
        owner_session_cluster="test-cluster",
        session_owner_token="o" * 32,
    )
    owned_queue = ClioCoreQueue(owned_settings.core_dir)
    unowned_job = owned_queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["true"]),
            idempotency_key="websocket-unowned-job",
        )
    )
    owned_client = cast(Any, TestClient(create_app(owned_settings)))
    owner_headers = {
        OWNER_SESSION_ID_HEADER: "desktop-session-1",
        SESSION_GENERATION_ID_HEADER: "generation-1",
    }

    cases: tuple[tuple[Any, str, dict[str, str], str], ...] = (
        (
            authenticated_client,
            "/jobs/missing-job/monitor/ws",
            {},
            "websocket_authentication_failed",
        ),
        (
            owned_client,
            "/jobs/missing-job/monitor/ws?token=api-token",
            {},
            "websocket_session_binding_failed",
        ),
        (
            plain_client,
            "/jobs/missing-job/monitor/ws?limit=1000000000000",
            {},
            "websocket_page_limit_invalid",
        ),
        (
            plain_client,
            "/jobs/missing-job/monitor/ws?poll_seconds=0",
            {},
            "websocket_poll_interval_invalid",
        ),
        (
            plain_client,
            "/jobs/missing-job/monitor/ws?cursor=0",
            {},
            "websocket_cursor_invalid",
        ),
        (
            owned_client,
            f"/jobs/{unowned_job.job_id}/monitor/ws?token=api-token",
            owner_headers,
            "websocket_resource_ownership_refused",
        ),
        (
            plain_client,
            "/jobs/missing-job/monitor/ws",
            {},
            "websocket_resource_not_found",
        ),
    )

    for client, path, headers, expected_reason in cases:
        with (
            pytest.raises(WebSocketDisconnect) as caught,
            client.websocket_connect(path, headers=headers),
        ):
            pass
        assert caught.value.code == 1008
        assert caught.value.reason == expected_reason


def test_http_api_enforces_configured_token(tmp_path: Path) -> None:
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
        api_token="secret-token",
    )
    queue = ClioCoreQueue(settings.core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_yaml="name: generic\npkgs: []\n"),
            idempotency_key="http-auth",
        )
    )
    client = cast(Any, TestClient(create_app(settings)))

    missing = client.get(f"/jobs/{job.job_id}")
    wrong = client.get(f"/jobs/{job.job_id}", headers={"Authorization": "Bearer wrong"})
    bearer = client.get(f"/jobs/{job.job_id}", headers={"Authorization": "Bearer secret-token"})
    explicit = client.get(f"/jobs/{job.job_id}", headers={"X-Clio-Relay-Token": "secret-token"})

    assert missing.status_code == 401
    assert wrong.status_code == 401
    assert bearer.status_code == 200
    assert explicit.status_code == 200


def test_http_typed_submit_endpoints_create_real_jobs(tmp_path: Path) -> None:
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
        spool_max_log_bytes_per_stream=50,
        spool_max_log_bytes_per_job=100,
        storage_minimum_free_bytes=0,
        storage_max_job_reservation_bytes=1_000,
        storage_job_core_allowance_bytes=20,
        storage_job_result_allowance_bytes=30,
    )
    queue = ClioCoreQueue(settings.core_dir)
    client = cast(Any, TestClient(create_app(settings)))
    prompt_path = tmp_path / "prompt.md"
    mcp_config_path = tmp_path / "mcp.toml"
    identity_headers = {
        OWNER_SESSION_ID_HEADER: "desktop-session-1",
        SESSION_GENERATION_ID_HEADER: "generation-1",
    }

    jarvis_response = client.post(
        "/jobs/jarvis",
        headers=identity_headers,
        json={
            "cluster": "test-cluster",
            "pipeline_yaml": "name: generic\npkgs: []\n",
            "idempotency_key": "http-typed-jarvis",
        },
    )
    jarvis_pipeline_response = client.post(
        "/jobs/jarvis-pipeline",
        headers=identity_headers,
        json={
            "cluster": "test-cluster",
            "pipeline_name": "site_simulation_4node",
            "idempotency_key": "http-typed-jarvis-pipeline",
        },
    )
    agent_response = client.post(
        "/jobs/remote-agent",
        headers=identity_headers,
        json={
            "cluster": "test-cluster",
            "prompt_path": str(prompt_path),
            "mcp_config_path": str(mcp_config_path),
            "model": "configured-model",
            "workdir": str(tmp_path),
            "timeout_seconds": 60,
            "idempotency_key": "http-typed-agent",
        },
    )
    removed_routes = [
        client.post(path, json={"cluster": "test-cluster", "idempotency_key": "removed"})
        for path in ("/jobs/mcp-call", "/jobs/jarvis-mcp-call")
    ]

    assert jarvis_response.status_code == 200
    assert jarvis_pipeline_response.status_code == 200
    assert agent_response.status_code == 200
    assert all(response.status_code in {404, 405} for response in removed_routes)
    jarvis = queue.get_job(jarvis_response.json()["job_id"])
    jarvis_pipeline = queue.get_job(jarvis_pipeline_response.json()["job_id"])
    agent = queue.get_job(agent_response.json()["job_id"])
    assert jarvis.kind == JobKind.JARVIS
    assert isinstance(jarvis.spec, JarvisRunSpec)
    assert isinstance(jarvis_pipeline.spec, JarvisRunSpec)
    assert jarvis_pipeline.spec.pipeline_name == "site_simulation_4node"
    assert agent.kind == JobKind.REMOTE_AGENT
    assert isinstance(agent.spec, RemoteAgentTaskSpec)
    assert agent.spec.prompt_path == str(prompt_path)
    assert agent.spec.mcp_config_path == str(mcp_config_path)
    assert agent.spec.model == "configured-model"


def test_job_submission_conflict_names_the_idempotency_key_move(tmp_path: Path) -> None:
    """clio-relay#242 actionability audit (R9 doctrine): a reused
    idempotency_key with a different payload must tell the agent the exact
    key at fault and that a fresh key is the way to submit a new request."""
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    client = cast(Any, TestClient(create_app(settings)))
    identity_headers = {
        OWNER_SESSION_ID_HEADER: "desktop-session-1",
        SESSION_GENERATION_ID_HEADER: "generation-1",
    }
    first = client.post(
        "/jobs/jarvis",
        headers=identity_headers,
        json={
            "cluster": "test-cluster",
            "pipeline_yaml": "name: first\npkgs: []\n",
            "idempotency_key": "reused-key",
        },
    )
    assert first.status_code == 200

    response = client.post(
        "/jobs/jarvis",
        headers=identity_headers,
        json={
            "cluster": "test-cluster",
            "pipeline_yaml": "name: different\npkgs: []\n",
            "idempotency_key": "reused-key",
        },
    )

    assert response.status_code == 409
    body = response.json()
    assert body["reason"] == "job_submission_conflict"
    assert "reused-key" in body["detail"]
    assert "not retryable" in body["detail"]
    assert "new idempotency_key" in body["detail"]


def test_http_remote_agent_submission_requires_typed_owner_session_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A schedulable agent lane must refuse missing attribution as typed data."""
    jobs: list[RelayJob] = []

    class SubmissionQueue:
        closed = False

        def initialize(self) -> None:
            pass

        def close(self) -> None:
            self.closed = True

        def submit_job(self, job: RelayJob) -> RelayJob:
            jobs.append(job)
            return job

    def submission_queue(_settings: RelaySettings) -> SubmissionQueue:
        return SubmissionQueue()

    monkeypatch.setattr(
        http_api_module,
        "storage_managed_queue",
        submission_queue,
    )
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
        api_token="shared-admission-token",
    )
    client = cast(Any, TestClient(create_app(settings)))
    payload = {
        "cluster": "test-cluster",
        "prompt_path": "/remote/prompt.md",
        "idempotency_key": "missing-owner-session-identity",
    }

    response = client.post(
        "/jobs/remote-agent",
        headers={"Authorization": "Bearer shared-admission-token"},
        json=payload,
    )
    jarvis_response = client.post(
        "/jobs/jarvis-pipeline",
        headers={"Authorization": "Bearer shared-admission-token"},
        json={
            "cluster": "test-cluster",
            "pipeline_name": "site-simulation",
            "idempotency_key": "missing-jarvis-owner-session-identity",
        },
    )
    identity_without_bearer = client.post(
        "/jobs/remote-agent",
        headers={
            OWNER_SESSION_ID_HEADER: "desktop-session-1",
            SESSION_GENERATION_ID_HEADER: "generation-1",
        },
        json={**payload, "idempotency_key": "identity-is-not-admission"},
    )

    assert response.status_code == 422
    assert jarvis_response.status_code == 422
    assert jarvis_response.json()["reason"] == "owner_session_identity_refused"
    assert jarvis_response.json()["code"] == "owner_session_identity_required"
    assert jarvis_response.json()["job_kind"] == "jarvis"
    assert identity_without_bearer.status_code == 401
    assert response.json()["reason"] == "owner_session_identity_refused"
    assert {
        key: response.json()[key]
        for key in ("schema", "code", "job_kind", "required_headers", "message")
    } == {
        "schema": "clio-relay.owner-session-identity-error.v1",
        "code": "owner_session_identity_required",
        "job_kind": "remote_agent",
        "required_headers": [
            OWNER_SESSION_ID_HEADER,
            SESSION_GENERATION_ID_HEADER,
        ],
        "message": "remote_agent submissions require owner-session identity headers",
    }
    assert jobs == []


def test_http_job_lanes_record_owner_session_identity_when_supplied(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Job lanes retain the exact attribution pair and refuse a partial pair."""
    jobs: list[RelayJob] = []

    class SubmissionQueue:
        closed = False

        def initialize(self) -> None:
            pass

        def close(self) -> None:
            self.closed = True

        def submit_job(self, job: RelayJob) -> RelayJob:
            jobs.append(job)
            return job

    queue = SubmissionQueue()

    def submission_queue(_settings: RelaySettings) -> SubmissionQueue:
        return queue

    monkeypatch.setattr(
        http_api_module,
        "storage_managed_queue",
        submission_queue,
    )
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
        api_token="shared-admission-token",
    )
    client = cast(Any, TestClient(create_app(settings)))
    identity_headers = {
        "Authorization": "Bearer shared-admission-token",
        OWNER_SESSION_ID_HEADER: "desktop-session-1",
        SESSION_GENERATION_ID_HEADER: "generation-1",
    }

    jarvis = client.post(
        "/jobs/jarvis-pipeline",
        headers=identity_headers,
        json={
            "cluster": "test-cluster",
            "pipeline_name": "site-simulation",
            "idempotency_key": "attributed-jarvis",
        },
    )
    agent = client.post(
        "/jobs/remote-agent",
        headers=identity_headers,
        json={
            "cluster": "test-cluster",
            "prompt_path": "/remote/prompt.md",
            "idempotency_key": "attributed-agent",
        },
    )
    incomplete = client.post(
        "/jobs/remote-agent",
        headers={
            key: value
            for key, value in identity_headers.items()
            if key != SESSION_GENERATION_ID_HEADER
        },
        json={
            "cluster": "test-cluster",
            "prompt_path": "/remote/prompt.md",
            "idempotency_key": "incomplete-agent",
        },
    )

    assert [jarvis.status_code, agent.status_code] == [200, 200]
    assert len(jobs) == 2
    for job in jobs:
        assert job.owner_session_id == "desktop-session-1"
        assert job.owner_session_generation_id == "generation-1"
    assert incomplete.status_code == 422
    assert incomplete.json()["reason"] == "owner_session_identity_refused"
    assert incomplete.json()["code"] == "owner_session_identity_incomplete"


def test_http_progress_endpoints_record_and_list_progress(tmp_path: Path) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_yaml="name: generic\npkgs: []\n"),
            idempotency_key="http-progress",
        )
    )
    client = cast(Any, TestClient(create_app(settings)))

    record_response = client.post(
        f"/jobs/{job.job_id}/progress",
        json={
            "label": "iteration",
            "current": 5,
            "total": 10,
            "unit": "step",
            "message": "half way",
            "metadata": {
                "source": "jarvis_package",
                "adapter": "site-progress",
                "package_name": "site.simulation",
                "package_version": "2.1",
                "run_id": "spoofed",
            },
        },
    )
    list_response = client.get(f"/jobs/{job.job_id}/progress")

    assert record_response.status_code == 200
    assert list_response.status_code == 200
    recorded = record_response.json()
    listed = list_response.json()
    assert recorded["label"] == "iteration"
    assert recorded["current"] == 5
    assert recorded["metadata"]["source"] == "external_http"
    assert "package_name" not in recorded["metadata"]
    assert "run_id" not in recorded["metadata"]
    assert listed["progress"][0]["progress_id"] == recorded["progress_id"]
    assert listed["cursor"] == 1
    assert listed["limit"] == 100
    assert listed["next_cursor"] is None
    assert listed["total"] == 1


def test_http_lists_job_tasks(tmp_path: Path) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_yaml="name: generic\npkgs: []\n"),
            idempotency_key="http-tasks",
        )
    )
    task = queue.append_task(RelayTask(job_id=job.job_id, name="jarvis.execution"))
    client = cast(Any, TestClient(create_app(settings)))

    response = client.get(f"/jobs/{job.job_id}/tasks")

    assert response.status_code == 200
    page = response.json()
    assert page["tasks"][0]["task_id"] == task.task_id
    assert page["tasks"][0]["name"] == "jarvis.execution"
    assert page["cursor"] == 1
    assert page["limit"] == 100
    assert page["next_cursor"] is None
    assert page["total"] == 1


def test_http_task_timeline_events_are_replayable(tmp_path: Path) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.REMOTE_AGENT,
            spec=RemoteAgentTaskSpec(prompt_path="/tmp/prompt.md"),
            idempotency_key="http-task-events",
        )
    )
    task = queue.append_task(RelayTask(job_id=job.job_id, name="remote-agent.discovery"))
    client = cast(Any, TestClient(create_app(settings)))

    created = client.post(
        f"/tasks/{task.task_id}/events",
        json={
            "event_type": "dataset_found",
            "label": "dataset",
            "status": "succeeded",
            "summary": "Found staged dataset",
            "path_refs": ["/mnt/common/datasets/example_001"],
            "metadata": {"dataset": "example_001"},
        },
    )
    replay = client.get(f"/tasks/{task.task_id}/events", params={"cursor": 1})

    assert created.status_code == 200
    assert replay.status_code == 200
    assert created.json()["seq"] == 1
    assert replay.json()[0]["event_type"] == "dataset_found"
    assert replay.json()[0]["metadata"]["dataset"] == "example_001"


def test_http_task_timeline_sse_replays_existing_events(tmp_path: Path) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.REMOTE_AGENT,
            spec=RemoteAgentTaskSpec(prompt_path="/tmp/prompt.md"),
            idempotency_key="http-task-events-sse",
        )
    )
    task = queue.append_task(RelayTask(job_id=job.job_id, name="remote-agent.discovery"))
    queue.append_task_event(
        TaskTimelineEvent(
            task_id=task.task_id,
            event_type="repo_scan",
            label="repo",
            summary="Scanned visualization repository",
        )
    )
    client = cast(Any, TestClient(create_app(settings)))

    with client.stream(
        "GET",
        f"/tasks/{task.task_id}/events/sse",
        params={"poll_seconds": 0.01, "stop_after_replay": True},
    ) as response:
        body = response.read().decode("utf-8")

    assert response.status_code == 200
    assert "event: task_events" in body
    assert "repo_scan" in body


def test_http_task_timeline_rejects_invalid_cursor(tmp_path: Path) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.REMOTE_AGENT,
            spec=RemoteAgentTaskSpec(prompt_path="/tmp/prompt.md"),
            idempotency_key="http-task-events-invalid-cursor",
        )
    )
    task = queue.append_task(RelayTask(job_id=job.job_id, name="remote-agent.discovery"))
    client = cast(Any, TestClient(create_app(settings)))

    response = client.get(f"/tasks/{task.task_id}/events", params={"cursor": 0})

    assert response.status_code == 422


def test_http_gateway_session_lifecycle(tmp_path: Path) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    client = cast(Any, TestClient(create_app(settings)))

    created = client.post(
        "/gateway-sessions",
        json={
            "cluster": "test-cluster",
            "name": "live-service-example",
            "requested_resources": {"nodes": 1, "exclusive": True},
            "gateway": {"strategy": "ssh_forward", "remote_port": 11111},
        },
    )
    session_id = created.json()["session_id"]
    updated = client.patch(
        f"/gateway-sessions/{session_id}",
        json={
            "state": "ready",
            "node": "ares-comp-01",
            "gateway": {"strategy": "ssh_forward", "local_port": 5900},
            "metadata": {"dataset": "example_001"},
        },
    )
    listed = client.get("/gateway-sessions", params={"cluster": "test-cluster"})
    closed = client.post(f"/gateway-sessions/{session_id}/close")
    reopen = client.patch(f"/gateway-sessions/{session_id}", json={"state": "ready"})

    assert created.status_code == 200
    assert updated.status_code == 200
    assert listed.status_code == 200
    assert closed.status_code == 200
    assert updated.json()["state"] == GatewaySessionState.READY.value
    assert updated.json()["scheduler"] == "external"
    assert updated.json()["scheduler_job_id"] is None
    listed_page = listed.json()
    assert listed_page["gateway_sessions"][0]["session_id"] == session_id
    assert listed_page["source_cursor"] == 1
    assert listed_page["source_limit"] == 100
    assert listed_page["source_next_cursor"] is None
    assert listed_page["source_total"] == 1
    assert closed.json()["state"] == GatewaySessionState.CLOSED.value
    assert reopen.status_code == 409


@pytest.mark.parametrize(
    "payload",
    [
        {"scheduler": "slurm"},
        {"scheduler_job_id": "12345"},
        {"gateway": {"runtime_spec": {"kind": "forged"}}},
        {"gateway": {"jarvis_runtime_binding": {"schema_version": "forged"}}},
        {"gateway": {"scheduler_job_id": "12345"}},
        {"gateway": {"ownership_intents": {"scheduler_submission": {}}}},
        {"gateway": {"transport": {"remote_connector": {"pid": 42}}}},
        {"metadata": {"owner": "clio-relay"}},
        {"metadata": {"scheduler_provider": "slurm"}},
        {"metadata": {"owner_session_generation_id": "forged-generation"}},
    ],
)
def test_http_generic_gateway_create_rejects_relay_runtime_ownership_fields(
    tmp_path: Path,
    payload: dict[str, object],
) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    client = cast(Any, TestClient(create_app(settings)))

    response = client.post(
        "/gateway-sessions",
        json={"cluster": "test-cluster", "name": "forged-runtime", **payload},
    )

    assert response.status_code == 422


@pytest.mark.parametrize(
    "payload",
    [
        {"scheduler_job_id": "12345"},
        {"gateway": {"runtime_spec": {"kind": "forged"}}},
        {"gateway": {"jarvis_runtime_binding": {"schema_version": "forged"}}},
        {"gateway": {"transport": {"desktop_connector": {"pid": 42}}}},
        {"metadata": {"owner_session_id": "forged-session"}},
    ],
)
def test_http_generic_gateway_update_rejects_relay_runtime_ownership_fields(
    tmp_path: Path,
    payload: dict[str, object],
) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    client = cast(Any, TestClient(create_app(settings)))
    created = client.post(
        "/gateway-sessions",
        json={"cluster": "test-cluster", "name": "ordinary-gateway"},
    )

    response = client.patch(
        f"/gateway-sessions/{created.json()['session_id']}",
        json=payload,
    )

    assert created.status_code == 200
    assert response.status_code == 422


def test_http_generic_gateway_update_cannot_replace_relay_managed_runtime_state(
    tmp_path: Path,
) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    runtime = queue.create_gateway_session(
        GatewaySession(
            cluster="test-cluster",
            name="relay-managed-runtime",
            gateway={
                "runtime_spec": {"kind": "image-service"},
                "ownership_intents": {"scheduler_submission": {"state": "recorded"}},
            },
            metadata={"owner": "clio-relay", "runtime_kind": "image-service"},
        )
    )
    client = cast(Any, TestClient(create_app(settings)))

    response = client.patch(
        f"/gateway-sessions/{runtime.session_id}",
        json={"gateway": {"strategy": "ssh_forward"}},
    )

    assert response.status_code == 409
    assert queue.get_gateway_session(runtime.session_id).gateway == runtime.gateway


def test_owned_session_api_fails_closed_without_cluster_or_owner_token(tmp_path: Path) -> None:
    with pytest.raises(ConfigurationError, match="OWNER_SESSION_CLUSTER"):
        create_app(
            RelaySettings(
                core_dir=tmp_path / "core",
                spool_dir=tmp_path / "spool",
                api_token="session-api-token",
                owner_session_id="desktop-session-1",
                owner_session_generation_id="generation-1",
            )
        )
    with pytest.raises(ConfigurationError, match="SESSION_OWNER_TOKEN"):
        create_app(
            RelaySettings(
                core_dir=tmp_path / "core",
                spool_dir=tmp_path / "spool",
                api_token="session-api-token",
                owner_session_id="desktop-session-1",
                owner_session_generation_id="generation-1",
                owner_session_cluster="test-cluster",
            )
        )
    with pytest.raises(ConfigurationError, match="at least 32 bytes"):
        create_app(
            RelaySettings(
                core_dir=tmp_path / "core",
                spool_dir=tmp_path / "spool",
                api_token="session-api-token",
                owner_session_id="desktop-session-1",
                owner_session_generation_id="generation-1",
                owner_session_cluster="test-cluster",
                session_owner_token="weak-token",
            )
        )


def test_owned_session_api_allows_explicit_unauthenticated_loopback_session(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _bind_owned_session_cluster_authority(monkeypatch, tmp_path)
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
        owner_session_id="desktop-session-1",
        owner_session_generation_id="generation-1",
        owner_session_cluster="test-cluster",
        session_owner_token="o" * 32,
        allow_unauthenticated_owned_session=True,
    )

    response = cast(Any, TestClient(create_app(settings))).get("/healthz")

    assert response.status_code == 200
    assert response.json() == {"ok": True, "auth": False, "console_sse": True}


def test_owned_session_api_reports_missing_server_generation_identity(
    tmp_path: Path,
) -> None:
    app = create_app(RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool"))

    @app.get("/__missing_server_generation")
    def missing_server_generation() -> None:  # pyright: ignore[reportUnusedFunction]
        raise door_errors_module.http_problem(
            "session_generation_identity_unavailable",
            "relay session has no exact generation identity",
        )

    response = cast(Any, TestClient(app)).get("/__missing_server_generation")

    assert response.status_code == 409
    assert response.json()["reason"] == "session_generation_identity_unavailable"


def test_owned_session_api_fails_closed_without_exact_process_authority(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
        api_token="session-api-token",
        owner_session_id="desktop-session-1",
        owner_session_generation_id="generation-1",
        owner_session_cluster="test-cluster",
        session_owner_token="o" * 32,
    )

    with pytest.raises(ConfigurationError, match="process-bound cluster authority"):
        create_app(settings)

    _bind_owned_session_cluster_authority(monkeypatch, tmp_path)
    monkeypatch.delenv("CLIO_RELAY_SESSION_ROUTE_REVISION")
    with pytest.raises(ConfigurationError, match="must be configured together"):
        create_app(settings)

    _bind_owned_session_cluster_authority(monkeypatch, tmp_path)
    monkeypatch.setenv(CLUSTER_REGISTRY_ENV, "")
    with pytest.raises(ConfigurationError, match="path must not be blank"):
        create_app(settings)


def test_owned_session_api_rejects_invalid_or_ambiguous_process_authority(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    definition = ClusterDefinition(name="test-cluster", ssh_host="test-cluster")
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
        api_token="session-api-token",
        owner_session_id="desktop-session-1",
        owner_session_generation_id="generation-1",
        owner_session_cluster="test-cluster",
        session_owner_token="o" * 32,
    )

    _bind_owned_session_cluster_authority(monkeypatch, tmp_path, definition=definition)
    monkeypatch.setenv("CLIO_RELAY_SESSION_REGISTRY_SHA256", "invalid")
    with pytest.raises(ConfigurationError, match="registry SHA-256 is invalid"):
        create_app(settings)

    _bind_owned_session_cluster_authority(monkeypatch, tmp_path, definition=definition)
    monkeypatch.setenv("CLIO_RELAY_SESSION_ROUTE_REVISION", "invalid")
    with pytest.raises(ConfigurationError, match="route revision is invalid"):
        create_app(settings)

    _bind_owned_session_cluster_authority(monkeypatch, tmp_path, definition=definition)
    monkeypatch.setenv("CLIO_RELAY_SESSION_REGISTRY_SHA256", "f" * 64)
    with pytest.raises(ConfigurationError, match="registry digest does not match"):
        create_app(settings)

    _bind_owned_session_cluster_authority(monkeypatch, tmp_path, definition=definition)
    monkeypatch.setenv("CLIO_RELAY_SESSION_ROUTE_REVISION", "f" * 64)
    with pytest.raises(ConfigurationError, match="route revision does not match"):
        create_app(settings)

    _bind_owned_session_cluster_authority(monkeypatch, tmp_path, definition=definition)
    registry_path = Path(os.environ[CLUSTER_REGISTRY_ENV])
    ClusterRegistry(
        clusters={
            "test-cluster": definition,
            "other-cluster": ClusterDefinition(
                name="other-cluster",
                ssh_host="other-cluster",
            ),
        }
    ).save(registry_path)
    monkeypatch.setenv(
        "CLIO_RELAY_SESSION_REGISTRY_SHA256",
        hashlib.sha256(registry_path.read_bytes()).hexdigest(),
    )
    with pytest.raises(ConfigurationError, match="exactly the owner session cluster"):
        create_app(settings)


def test_normal_desktop_api_accepts_operator_registry_without_session_markers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    definition = ClusterDefinition(name="alpha", ssh_host="alpha-login")
    registry_path = tmp_path / "desktop-registry" / "clusters.json"
    ClusterRegistry(clusters={"alpha": definition}).save(registry_path)
    monkeypatch.setenv(CLUSTER_REGISTRY_ENV, str(registry_path))
    monkeypatch.delenv("CLIO_RELAY_SESSION_REGISTRY_SHA256", raising=False)
    monkeypatch.delenv("CLIO_RELAY_SESSION_ROUTE_REVISION", raising=False)
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    client = cast(Any, TestClient(create_app(settings)))

    response = client.post(
        "/jobs/jarvis-pipeline",
        headers={
            OWNER_SESSION_ID_HEADER: "desktop-session-1",
            SESSION_GENERATION_ID_HEADER: "generation-1",
        },
        json={
            "cluster": "alpha",
            "pipeline_name": "site-simulation",
            "idempotency_key": "desktop-alpha-pipeline",
        },
    )

    assert response.status_code == 200
    job = queue.get_job(response.json()["job_id"])
    assert job.cluster == "alpha"
    assert isinstance(job.spec, JarvisRunSpec)


def test_owned_session_identity_challenge_is_public_and_exact(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _bind_owned_session_cluster_authority(monkeypatch, tmp_path)
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
        api_token="session-api-token",
        owner_session_id="desktop-session-1",
        owner_session_generation_id="generation-1",
        remote_cluster="test-cluster",
        session_owner_token="o" * 32,
    )
    client = cast(Any, TestClient(create_app(settings)))
    nonce = "1" * 64

    response = client.get("/session-identity", params={"nonce": nonce})

    assert response.status_code == 200
    assert response.json() == session_identity_document(
        owner_token="o" * 32,
        cluster="test-cluster",
        session_id="desktop-session-1",
        generation_id="generation-1",
        nonce=nonce,
    )
    invalid_nonce = client.get("/session-identity", params={"nonce": "not-a-nonce"})
    assert invalid_nonce.status_code == 422
    assert invalid_nonce.json()["reason"] == "request_validation_failed"
    assert invalid_nonce.json()["status"] == 422
    assert invalid_nonce.json()["detail"] == "Request validation failed."


def test_framework_http_errors_are_owner_rendered_without_status_drift(tmp_path: Path) -> None:
    app = create_app(RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool"))

    @app.get("/__framework_teapot")
    def framework_teapot() -> None:  # pyright: ignore[reportUnusedFunction]
        raise StarletteHTTPException(status_code=418, detail="framework internals")

    client = cast(Any, TestClient(app))
    responses = (
        (client.get("/__missing_route"), 404, "route_not_found"),
        (client.post("/healthz"), 405, "method_not_allowed"),
        (client.get("/__framework_teapot"), 418, "framework_http_error"),
    )

    for response, expected_status, expected_reason in responses:
        assert response.status_code == expected_status
        assert response.headers["content-type"].startswith("application/problem+json")
        document = response.json()
        assert document["schema_version"] == "clio-relay.error.v1"
        assert document["status"] == expected_status
        assert document["reason"] == expected_reason
        assert "framework internals" not in document["detail"]


def test_owned_session_api_stamps_jobs_and_gateways_with_server_ownership(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _bind_owned_session_cluster_authority(monkeypatch, tmp_path)
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
        api_token="session-api-token",
        owner_session_id="desktop-session-1",
        owner_session_generation_id="generation-1",
        remote_cluster="test-cluster",
        session_owner_token="o" * 32,
    )
    queue = ClioCoreQueue(settings.core_dir)
    assert (
        queue.prepare_owner_session_start(
            "desktop-session-1",
            recorded_generation_id=None,
            candidate_generation_id="generation-1",
        )
        == "generation-1"
    )
    client = cast(
        Any,
        TestClient(
            create_app(settings),
            headers={
                "Authorization": "Bearer session-api-token",
                OWNER_SESSION_ID_HEADER: "desktop-session-1",
                SESSION_GENERATION_ID_HEADER: "generation-1",
            },
        ),
    )
    raw_job = RelayJob(
        cluster="test-cluster",
        kind=JobKind.JARVIS,
        spec=JarvisRunSpec(command=["sleep", "60"]),
        idempotency_key="owned-http-job",
    )

    submitted = client.post("/jobs", json=raw_job.model_dump(mode="json"))
    forged_payload = raw_job.model_dump(mode="json")
    forged_payload["metadata"] = {
        "owner": "untrusted-client",
        "owner_session_id": "forged-session",
        "owner_session_generation_id": "forged-generation",
        "owner_session_admission_id": "forged-admission",
    }
    forged = client.post("/jobs", json=forged_payload)
    gateway = client.post(
        "/gateway-sessions",
        json={
            "cluster": "test-cluster",
            "name": "owned-gateway",
            "metadata": {"dataset": "example"},
        },
    )
    patched = client.patch(
        f"/gateway-sessions/{gateway.json()['session_id']}",
        json={"metadata": {"phase": "ready"}},
    )

    assert submitted.status_code == 200
    assert forged.status_code == 422
    assert "server-managed" in forged.json()["detail"]
    assert submitted.json()["owner_session_id"] == "desktop-session-1"
    assert submitted.json()["owner_session_generation_id"] == "generation-1"
    assert submitted.json()["metadata"] == {
        "owner": "clio-relay",
        "owner_session_id": "desktop-session-1",
        "owner_session_generation_id": "generation-1",
    }
    assert gateway.status_code == 200
    assert patched.status_code == 200
    assert gateway.json()["metadata"]["owner_session_generation_id"] == "generation-1"
    assert patched.json()["metadata"]["owner"] == "clio-relay"
    assert patched.json()["metadata"]["owner_session_id"] == "desktop-session-1"
    assert patched.json()["metadata"]["owner_session_generation_id"] == "generation-1"
    assert patched.json()["metadata"]["dataset"] == "example"
    assert patched.json()["metadata"]["phase"] == "ready"


def test_session_job_submission_rejects_missing_and_stale_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _bind_owned_session_cluster_authority(monkeypatch, tmp_path)
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
        api_token="session-api-token",
        owner_session_id="desktop-session-1",
        owner_session_generation_id="generation-1",
        remote_cluster="test-cluster",
        session_owner_token="o" * 32,
    )
    queue = ClioCoreQueue(settings.core_dir)
    queue.prepare_owner_session_start(
        "desktop-session-1",
        recorded_generation_id=None,
        candidate_generation_id="generation-1",
    )
    client = cast(Any, TestClient(create_app(settings)))
    payload = {
        "cluster": "test-cluster",
        "pipeline_name": "pipeline",
        "idempotency_key": "session-header-adversarial",
    }
    authorization = {"Authorization": "Bearer session-api-token"}

    missing = client.post("/jobs/jarvis-pipeline", headers=authorization, json=payload)
    stale = client.post(
        "/jobs/jarvis-pipeline",
        headers={
            **authorization,
            OWNER_SESSION_ID_HEADER: "desktop-session-1",
            SESSION_GENERATION_ID_HEADER: "generation-0",
        },
        json=payload,
    )
    wrong_session = client.post(
        "/jobs/jarvis-pipeline",
        headers={
            **authorization,
            OWNER_SESSION_ID_HEADER: "desktop-session-2",
            SESSION_GENERATION_ID_HEADER: "generation-1",
        },
        json=payload,
    )
    gateway_payload = {"cluster": "test-cluster", "name": "bound-gateway"}
    missing_gateway = client.post(
        "/gateway-sessions",
        headers=authorization,
        json=gateway_payload,
    )
    stale_gateway = client.post(
        "/gateway-sessions",
        headers={
            **authorization,
            OWNER_SESSION_ID_HEADER: "desktop-session-1",
            SESSION_GENERATION_ID_HEADER: "generation-0",
        },
        json=gateway_payload,
    )
    wrong_cluster_gateway = client.post(
        "/gateway-sessions",
        headers={
            **authorization,
            OWNER_SESSION_ID_HEADER: "desktop-session-1",
            SESSION_GENERATION_ID_HEADER: "generation-1",
        },
        json={"cluster": "other-cluster", "name": "wrong-cluster"},
    )

    assert missing.status_code == 409
    assert missing.json()["reason"] == "session_binding_headers_required"
    assert stale.status_code == 409
    assert stale.json()["reason"] == "session_binding_identity_mismatch"
    assert wrong_session.status_code == 409
    assert wrong_session.json()["reason"] == "session_binding_identity_mismatch"
    assert missing_gateway.status_code == 409
    assert missing_gateway.json()["reason"] == "session_binding_headers_required"
    assert stale_gateway.status_code == 409
    assert stale_gateway.json()["reason"] == "session_binding_identity_mismatch"
    assert wrong_cluster_gateway.status_code == 409
    assert queue.list_jobs() == []
    assert queue.list_gateway_sessions() == []

    monkeypatch.delenv(CLUSTER_REGISTRY_ENV)
    monkeypatch.delenv("CLIO_RELAY_SESSION_REGISTRY_SHA256")
    monkeypatch.delenv("CLIO_RELAY_SESSION_ROUTE_REVISION")
    unbound_settings = RelaySettings(
        core_dir=tmp_path / "unbound-core",
        spool_dir=tmp_path / "unbound-spool",
        api_token="session-api-token",
        owner_session_id="desktop-session-1",
        owner_session_generation_id="generation-1",
        remote_cluster="test-cluster",
        session_owner_token="o" * 32,
    )
    with pytest.raises(
        ConfigurationError,
        match="owned relay session API requires process-bound cluster authority",
    ):
        create_app(unbound_settings)


def test_owned_session_submission_race_with_quiesce_returns_conflict(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _bind_owned_session_cluster_authority(monkeypatch, tmp_path)
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
        api_token="session-api-token",
        owner_session_id="desktop-session-1",
        owner_session_generation_id="generation-1",
        remote_cluster="test-cluster",
        session_owner_token="o" * 32,
    )
    queue = ClioCoreQueue(settings.core_dir)
    queue.prepare_owner_session_start(
        "desktop-session-1",
        recorded_generation_id=None,
        candidate_generation_id="generation-1",
    )
    original_submit = StorageManagedQueue.submit_job

    def quiesce_before_commit(self: StorageManagedQueue, job: RelayJob) -> RelayJob:
        self.set_owner_session_closing(
            "desktop-session-1",
            session_generation_id="generation-1",
        )
        return original_submit(self, job)

    monkeypatch.setattr(StorageManagedQueue, "submit_job", quiesce_before_commit)
    client = cast(
        Any,
        TestClient(
            create_app(settings),
            headers={
                "Authorization": "Bearer session-api-token",
                OWNER_SESSION_ID_HEADER: "desktop-session-1",
                SESSION_GENERATION_ID_HEADER: "generation-1",
            },
        ),
    )

    response = client.post(
        "/jobs/jarvis-pipeline",
        json={
            "cluster": "test-cluster",
            "pipeline_name": "pipeline",
            "idempotency_key": "quiesce-race",
        },
    )

    assert response.status_code == 409
    assert queue.list_jobs() == []


def test_owned_session_api_cannot_take_over_or_close_other_gateways(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _bind_owned_session_cluster_authority(monkeypatch, tmp_path)
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
        api_token="session-api-token",
        owner_session_id="desktop-session-1",
        owner_session_generation_id="generation-1",
        remote_cluster="test-cluster",
        session_owner_token="o" * 32,
    )
    queue = ClioCoreQueue(settings.core_dir)
    assert (
        queue.prepare_owner_session_start(
            "desktop-session-2",
            recorded_generation_id=None,
            candidate_generation_id="generation-2",
        )
        == "generation-2"
    )
    other_owned = queue.create_gateway_session(
        GatewaySession(
            cluster="test-cluster",
            name="other-owned",
            metadata={
                "owner": "clio-relay",
                "owner_session_id": "desktop-session-2",
                "owner_session_generation_id": "generation-2",
            },
        )
    )
    assert (
        queue.prepare_owner_session_start(
            "desktop-session-1",
            recorded_generation_id=None,
            candidate_generation_id="generation-0",
        )
        == "generation-0"
    )
    prior_generation = queue.create_gateway_session(
        GatewaySession(
            cluster="test-cluster",
            name="prior-generation",
            metadata={
                "owner": "clio-relay",
                "owner_session_id": "desktop-session-1",
                "owner_session_generation_id": "generation-0",
            },
        )
    )
    prior_generation = queue.close_gateway_session(prior_generation.session_id)
    queue.set_owner_session_closing(
        "desktop-session-1",
        session_generation_id="generation-0",
    )
    queue.set_owner_session_closed(
        "desktop-session-1",
        session_generation_id="generation-0",
    )
    assert (
        queue.prepare_owner_session_start(
            "desktop-session-1",
            recorded_generation_id="generation-0",
            candidate_generation_id="generation-1",
        )
        == "generation-1"
    )
    unowned = queue.create_gateway_session(GatewaySession(cluster="test-cluster", name="unowned"))
    client = cast(
        Any,
        TestClient(
            create_app(settings),
            headers={
                "Authorization": "Bearer session-api-token",
                OWNER_SESSION_ID_HEADER: "desktop-session-1",
                SESSION_GENERATION_ID_HEADER: "generation-1",
            },
        ),
    )

    for session in (other_owned, prior_generation, unowned):
        patched = client.patch(
            f"/gateway-sessions/{session.session_id}",
            json={"metadata": {"phase": "forged"}},
        )
        closed = client.post(f"/gateway-sessions/{session.session_id}/close")

        assert patched.status_code == 403
        assert closed.status_code == 403
        unchanged = queue.get_gateway_session(session.session_id)
        assert unchanged.state is session.state
        assert unchanged.metadata == session.metadata


def test_owned_session_api_filters_jobs_redacts_capabilities_and_quiesces_intake(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _bind_owned_session_cluster_authority(monkeypatch, tmp_path)
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
        api_token="session-api-token",
        owner_session_id="desktop-session-1",
        owner_session_generation_id="generation-1",
        remote_cluster="test-cluster",
        session_owner_token="o" * 32,
    )
    queue = ClioCoreQueue(settings.core_dir)
    owned = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["sleep", "60"]),
            idempotency_key="owned-session-visible",
            metadata={
                "owner": "clio-relay",
                "owner_session_id": "desktop-session-1",
                "owner_session_generation_id": "generation-1",
                "owner_token": "private-capability",
            },
        )
    )
    other = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["sleep", "60"]),
            idempotency_key="other-session-hidden",
            metadata={
                "owner": "clio-relay",
                "owner_session_id": "desktop-session-2",
                "owner_session_generation_id": "generation-2",
            },
        )
    )
    prior_generation = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["sleep", "60"]),
            idempotency_key="prior-generation-hidden",
            metadata={
                "owner": "clio-relay",
                "owner_session_id": "desktop-session-1",
                "owner_session_generation_id": "generation-0",
            },
        )
    )
    client = cast(
        Any,
        TestClient(
            create_app(settings),
            headers={
                "Authorization": "Bearer session-api-token",
                OWNER_SESSION_ID_HEADER: "desktop-session-1",
                SESSION_GENERATION_ID_HEADER: "generation-1",
            },
        ),
    )

    owned_response = client.get(f"/jobs/{owned.job_id}")
    other_response = client.get(f"/jobs/{other.job_id}")
    other_status = client.get(f"/jobs/{other.job_id}/status")
    prior_generation_response = client.get(f"/jobs/{prior_generation.job_id}")
    listing = client.get("/queue")

    assert owned_response.status_code == 200
    assert owned_response.json()["metadata"]["owner_token"] == "<redacted>"
    assert queue.get_job(owned.job_id).metadata["owner_token"] == "private-capability"
    assert other_response.status_code == 403
    assert other_status.status_code == 403
    assert prior_generation_response.status_code == 403
    assert listing.status_code == 200
    assert listing.json()["count"] == 1
    assert listing.json()["jobs"][0]["job"]["job_id"] == owned.job_id
    assert client.get(f"/queue/jobs/{owned.job_id}/diagnose").status_code == 200
    assert client.get(f"/queue/jobs/{other.job_id}/diagnose").status_code == 403
    assert client.get("/queue/diagnostics").status_code == 403
    assert client.get("/queue/stale", params={"cluster": "test-cluster"}).status_code == 403
    assert client.get("/workers").status_code == 403

    queue.prepare_owner_session_start(
        "desktop-session-1",
        recorded_generation_id=None,
        candidate_generation_id="generation-1",
    )
    queue.set_owner_session_closing(
        "desktop-session-1",
        session_generation_id="generation-1",
    )
    new_job = RelayJob(
        cluster="test-cluster",
        kind=JobKind.JARVIS,
        spec=JarvisRunSpec(command=["true"]),
        idempotency_key="rejected-after-quiesce",
    )
    rejected_job = client.post("/jobs", json=new_job.model_dump(mode="json"))
    rejected_gateway = client.post(
        "/gateway-sessions",
        json={"cluster": "test-cluster", "name": "rejected-after-quiesce"},
    )
    assert rejected_job.status_code == 409
    assert rejected_job.json()["reason"] == "session_intake_closed"
    assert rejected_gateway.status_code == 409
    assert rejected_gateway.json()["reason"] == "session_intake_closed"


def test_http_job_status_includes_relay_queue_and_scheduler(tmp_path: Path) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_yaml="name: generic\npkgs: []\n"),
            idempotency_key="http-status",
        )
    )
    task = queue.append_task(RelayTask(job_id=job.job_id, name="jarvis.execution"))
    queue.update_task_metadata(
        task.task_id,
        {
            "scheduler_status": {
                "scheduler": "slurm",
                "scheduler_job_id": "100",
                "phase": "pending",
            }
        },
    )
    client = cast(Any, TestClient(create_app(settings)))

    response = client.get(f"/jobs/{job.job_id}/status")

    assert response.status_code == 200
    payload = response.json()
    assert payload["relay_queue"]["position"] == 1
    assert payload["scheduler"][0]["status"]["phase"] == "pending"


def test_http_queue_management_routes(tmp_path: Path) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    queue.register_endpoint(
        EndpointRegistration(
            role=EndpointRole.WORKER,
            cluster="test-cluster",
            hostname="node",
            pid=123,
            metadata={"concurrency": 2},
        )
    )
    job = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_yaml="name: generic\npkgs: []\n"),
            idempotency_key="http-queue-management",
        )
    )
    queue.acquire_next_job("endpoint-1", cluster="test-cluster", ttl_seconds=-1)
    client = cast(Any, TestClient(create_app(settings)))

    listed = client.get(
        "/queue",
        params={
            "cluster": "test-cluster",
            "kind": "jarvis",
            "limit": 1,
            "scan_limit": 1,
        },
    )
    diagnosed = client.get("/queue/diagnostics", params={"cluster": "test-cluster"})
    specific = client.get(
        f"/queue/jobs/{job.job_id}/diagnose",
        params={"cluster": "test-cluster", "older_than_seconds": 3600},
    )
    stale = client.get(
        "/queue/stale",
        params={
            "cluster": "test-cluster",
            "older_than_seconds": 3600,
            "kind": "jarvis",
        },
    )
    workers = client.get("/workers", params={"cluster": "test-cluster"})
    cleanup = client.post(
        "/queue/cleanup-stale",
        params={"cluster": "test-cluster", "dry_run": False},
    )
    canceled = client.post(f"/queue/jobs/{job.job_id}/cancel")

    assert listed.status_code == 200
    assert diagnosed.status_code == 200
    assert specific.status_code == 200
    assert stale.status_code == 200
    assert workers.status_code == 200
    assert cleanup.status_code == 200
    assert canceled.status_code == 200
    assert listed.json()["count"] == 1
    assert diagnosed.json()["issues"][0]["code"] == "expired_lease"
    assert specific.json()["reason"] == "stale_lease"
    assert stale.json()["jobs"][0]["job"]["job_id"] == job.job_id
    assert workers.json()["configured_concurrency"] == 2
    assert cleanup.json()["recovered_count"] == 1
    assert canceled.json()["scheduler_policy"] == "relay-only"


def test_http_queue_job_routes_reject_cluster_mismatch(tmp_path: Path) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["echo", "hello"]),
            idempotency_key="http-cluster-mismatch",
        )
    )
    client = cast(Any, TestClient(create_app(settings)))

    diagnosis = client.get(
        f"/queue/jobs/{job.job_id}/diagnose",
        params={"cluster": "homelab"},
    )
    canceled = client.post(
        f"/queue/jobs/{job.job_id}/cancel",
        json={"cluster": "homelab"},
    )

    assert diagnosis.status_code == 409
    assert canceled.status_code == 409
    assert queue.get_job(job.job_id).state == JobState.QUEUED


def test_http_stale_exact_job_target_preserves_neighbor(tmp_path: Path) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    jobs = [
        queue.submit_job(
            RelayJob(
                cluster="test-cluster",
                kind=JobKind.JARVIS,
                spec=JarvisRunSpec(command=["true"]),
                idempotency_key=f"http-exact-stale-{index}",
            )
        )
        for index in range(2)
    ]
    old = utc_now() - timedelta(hours=3)
    for job in jobs:
        queue._write_job_unlocked(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            job.model_copy(update={"created_at": old, "updated_at": old})
        )
    client = cast(Any, TestClient(create_app(settings)))

    discovered = client.get(
        "/queue/stale",
        params={
            "cluster": "test-cluster",
            "job_id": jobs[0].job_id,
            "older_than_seconds": 60,
        },
    )
    cleaned = client.post(
        "/queue/cleanup-stale",
        params={
            "cluster": "test-cluster",
            "job_id": jobs[0].job_id,
            "older_than_seconds": 60,
            "cancel_queued": True,
            "dry_run": False,
        },
    )

    assert discovered.status_code == 200
    assert cleaned.status_code == 200
    assert [item["job"]["job_id"] for item in discovered.json()["jobs"]] == [jobs[0].job_id]
    assert [item["job_id"] for item in cleaned.json()["planned"]] == [jobs[0].job_id]
    assert queue.get_job(jobs[0].job_id).state is JobState.CANCELED
    assert queue.get_job(jobs[1].job_id).state is JobState.QUEUED


def test_http_healthz_does_not_require_token(tmp_path: Path) -> None:
    client = cast(
        Any,
        TestClient(
            create_app(
                RelaySettings(
                    core_dir=tmp_path / "core",
                    spool_dir=tmp_path / "spool",
                    api_token="secret-token",
                )
            )
        ),
    )

    response = client.get("/healthz")

    assert response.status_code == 200
    assert response.json() == {"ok": True, "auth": True, "console_sse": True}


def test_http_cancel_job_records_cancel_request(tmp_path: Path) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_yaml="name: generic\npkgs: []\n"),
            idempotency_key="http-cancel",
        )
    )
    client = cast(Any, TestClient(create_app(settings)))

    response = client.post(f"/jobs/{job.job_id}/cancel")
    events, _ = queue.drain_events(Cursor(job_id=job.job_id), limit=20)

    assert response.status_code == 200
    assert response.json()["state"] == "canceled"
    assert [event.event_type for event in events][-2:] == [
        "job.cancel_requested",
        "job.canceled",
    ]


def test_http_monitor_rules(tmp_path: Path) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_yaml="name: generic\npkgs: []\n"),
            idempotency_key="http-monitor",
        )
    )
    queue.append_event(job.job_id, "stdout.delta", "step 50", payload={"text": "step 50\n"})
    client = cast(Any, TestClient(create_app(settings)))

    create_response = client.post(
        "/monitor/rules",
        json=MonitorRule(job_id=job.job_id, pattern="step 50").model_dump(mode="json"),
    )
    list_response = client.get("/monitor/rules", params={"job_id": job.job_id})
    run_response = client.post("/monitor/run-once")

    assert create_response.status_code == 200
    assert list_response.status_code == 200
    assert list_response.json()["rules"][0]["job_id"] == job.job_id
    assert list_response.json()["source_cursor"] == 1
    assert list_response.json()["source_total"] == 1
    assert run_response.status_code == 200
    assert run_response.json()[0]["action"] == "emit_event"
