"""Owned-session client-liveness lease: storage, sweep, and typed events.

iowarp/clio-relay#277: lease created on session bring-up; renewed by API
traffic (see test_owned_session_channel.py for the HTTP chokepoint itself);
clean close vs expiry are two typed, distinguishable shapes; expiry runs
termination + admission closure with zero manual action; expiry with
running jobs marks and preserves them.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pytest

from clio_relay.config import RelaySettings
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.endpoint import EndpointWorker
from clio_relay.errors import QueueConflictError, RelayError
from clio_relay.models import EndpointRole, JobKind, JobState, RelayJob
from clio_relay.models_job_specs import JarvisRunSpec


def _queue(tmp_path: Path) -> ClioCoreQueue:
    queue = ClioCoreQueue(tmp_path / "core")
    queue.initialize()
    return queue


def _start_generation(queue: ClioCoreQueue, *, owner_session_id: str, generation_id: str) -> None:
    queue.prepare_owner_session_start(
        owner_session_id,
        recorded_generation_id=None,
        candidate_generation_id=generation_id,
    )


def test_touch_creates_an_open_lease(tmp_path: Path) -> None:
    queue = _queue(tmp_path)
    opened_at = datetime(2026, 1, 1, tzinfo=UTC)

    lease = queue.touch_owner_session_lease(
        "desktop-session-1",
        session_generation_id="generation-1",
        cluster="ares",
        ttl_seconds=1800,
        now=opened_at,
    )

    assert lease.status == "open"
    assert lease.owner_session_id == "desktop-session-1"
    assert lease.session_generation_id == "generation-1"
    assert lease.cluster == "ares"
    assert lease.ttl_seconds == 1800
    assert lease.opened_at == opened_at
    assert lease.last_seen_at == opened_at
    assert lease.closed_at is None
    assert lease.close_reason is None
    assert lease.expired_with_running_jobs is False


def test_touch_renews_last_seen_at_without_changing_opened_at(tmp_path: Path) -> None:
    queue = _queue(tmp_path)
    opened_at = datetime(2026, 1, 1, tzinfo=UTC)
    later = opened_at + timedelta(minutes=5)

    queue.touch_owner_session_lease(
        "desktop-session-1",
        session_generation_id="generation-1",
        cluster="ares",
        ttl_seconds=1800,
        now=opened_at,
    )
    renewed = queue.touch_owner_session_lease(
        "desktop-session-1",
        session_generation_id="generation-1",
        cluster="ares",
        ttl_seconds=1800,
        now=later,
    )

    assert renewed.opened_at == opened_at
    assert renewed.last_seen_at == later
    assert renewed.status == "open"


def test_touch_for_a_new_generation_opens_a_fresh_lease(tmp_path: Path) -> None:
    queue = _queue(tmp_path)
    t0 = datetime(2026, 1, 1, tzinfo=UTC)

    queue.touch_owner_session_lease(
        "desktop-session-1",
        session_generation_id="generation-1",
        cluster="ares",
        ttl_seconds=1800,
        now=t0,
    )
    queue.close_owner_session_lease(
        "desktop-session-1",
        session_generation_id="generation-1",
        reason="client_close",
        now=t0,
    )

    fresh = queue.touch_owner_session_lease(
        "desktop-session-1",
        session_generation_id="generation-2",
        cluster="ares",
        ttl_seconds=1800,
        now=t0 + timedelta(hours=1),
    )

    assert fresh.session_generation_id == "generation-2"
    assert fresh.status == "open"
    assert fresh.close_reason is None


def test_touch_on_an_already_terminal_lease_is_a_no_op(tmp_path: Path) -> None:
    """A renewal racing a concurrent close must never resurrect a terminal lease."""
    queue = _queue(tmp_path)
    t0 = datetime(2026, 1, 1, tzinfo=UTC)

    queue.touch_owner_session_lease(
        "desktop-session-1",
        session_generation_id="generation-1",
        cluster="ares",
        ttl_seconds=1800,
        now=t0,
    )
    closed = queue.close_owner_session_lease(
        "desktop-session-1",
        session_generation_id="generation-1",
        reason="client_close",
        now=t0,
    )
    assert closed is not None

    raced = queue.touch_owner_session_lease(
        "desktop-session-1",
        session_generation_id="generation-1",
        cluster="ares",
        ttl_seconds=1800,
        now=t0 + timedelta(seconds=5),
    )

    assert raced.status == "closed"
    assert raced.last_seen_at == t0  # unchanged -- never reopened


def test_owner_session_lease_status_returns_none_when_absent(tmp_path: Path) -> None:
    queue = _queue(tmp_path)
    assert queue.owner_session_lease_status("no-such-session") is None


def test_owner_session_lease_status_filters_by_generation(tmp_path: Path) -> None:
    queue = _queue(tmp_path)
    queue.touch_owner_session_lease(
        "desktop-session-1",
        session_generation_id="generation-1",
        cluster="ares",
        ttl_seconds=1800,
    )

    assert (
        queue.owner_session_lease_status("desktop-session-1", session_generation_id="generation-1")
        is not None
    )
    assert (
        queue.owner_session_lease_status("desktop-session-1", session_generation_id="generation-9")
        is None
    )


def test_close_client_close_marks_closed_not_expired(tmp_path: Path) -> None:
    queue = _queue(tmp_path)
    t0 = datetime(2026, 1, 1, tzinfo=UTC)
    queue.touch_owner_session_lease(
        "desktop-session-1",
        session_generation_id="generation-1",
        cluster="ares",
        ttl_seconds=1800,
        now=t0,
    )

    closed = queue.close_owner_session_lease(
        "desktop-session-1",
        session_generation_id="generation-1",
        reason="client_close",
        now=t0 + timedelta(seconds=1),
    )

    assert closed is not None
    assert closed.status == "closed"
    assert closed.close_reason == "client_close"
    assert closed.expired_with_running_jobs is False
    assert closed.running_job_ids_at_close == []
    assert closed.closed_at == t0 + timedelta(seconds=1)


def test_close_lease_expired_with_running_jobs_marks_and_preserves(tmp_path: Path) -> None:
    queue = _queue(tmp_path)
    queue.touch_owner_session_lease(
        "desktop-session-1",
        session_generation_id="generation-1",
        cluster="ares",
        ttl_seconds=1800,
    )

    expired = queue.close_owner_session_lease(
        "desktop-session-1",
        session_generation_id="generation-1",
        reason="lease_expired",
        running_job_ids=["job_b", "job_a", "job_a"],
    )

    assert expired is not None
    assert expired.status == "expired"
    assert expired.close_reason == "lease_expired"
    assert expired.expired_with_running_jobs is True
    assert expired.running_job_ids_at_close == ["job_a", "job_b"]


def test_close_lease_expired_without_running_jobs_does_not_mark(tmp_path: Path) -> None:
    queue = _queue(tmp_path)
    queue.touch_owner_session_lease(
        "desktop-session-1",
        session_generation_id="generation-1",
        cluster="ares",
        ttl_seconds=1800,
    )

    expired = queue.close_owner_session_lease(
        "desktop-session-1",
        session_generation_id="generation-1",
        reason="lease_expired",
        running_job_ids=[],
    )

    assert expired is not None
    assert expired.status == "expired"
    assert expired.expired_with_running_jobs is False


def test_close_is_idempotent_for_a_matching_retry(tmp_path: Path) -> None:
    queue = _queue(tmp_path)
    queue.touch_owner_session_lease(
        "desktop-session-1",
        session_generation_id="generation-1",
        cluster="ares",
        ttl_seconds=1800,
    )
    first = queue.close_owner_session_lease(
        "desktop-session-1",
        session_generation_id="generation-1",
        reason="client_close",
    )
    second = queue.close_owner_session_lease(
        "desktop-session-1",
        session_generation_id="generation-1",
        reason="client_close",
    )
    assert first == second


def test_close_refuses_to_relabel_a_terminal_lease(tmp_path: Path) -> None:
    """Clean close and expiry are two DISTINCT, never-conflated typed paths."""
    queue = _queue(tmp_path)
    queue.touch_owner_session_lease(
        "desktop-session-1",
        session_generation_id="generation-1",
        cluster="ares",
        ttl_seconds=1800,
    )
    queue.close_owner_session_lease(
        "desktop-session-1",
        session_generation_id="generation-1",
        reason="client_close",
    )

    with pytest.raises(QueueConflictError, match="different reason"):
        queue.close_owner_session_lease(
            "desktop-session-1",
            session_generation_id="generation-1",
            reason="lease_expired",
        )


def test_close_returns_none_when_no_lease_record_exists(tmp_path: Path) -> None:
    """A pre-#277 session (never touched by the HTTP chokepoint) is not an error."""
    queue = _queue(tmp_path)
    result = queue.close_owner_session_lease(
        "never-touched-session",
        session_generation_id="generation-1",
        reason="client_close",
    )
    assert result is None


def test_due_expired_leases_filters_status_cluster_and_ttl(tmp_path: Path) -> None:
    queue = _queue(tmp_path)
    t0 = datetime(2026, 1, 1, tzinfo=UTC)

    # Due: open, past its own TTL.
    queue.touch_owner_session_lease(
        "session-due", session_generation_id="gen-1", cluster="ares", ttl_seconds=60, now=t0
    )
    # Not due: open, well within TTL.
    queue.touch_owner_session_lease(
        "session-fresh",
        session_generation_id="gen-1",
        cluster="ares",
        ttl_seconds=1800,
        now=t0,
    )
    # Not due: already closed.
    queue.touch_owner_session_lease(
        "session-closed", session_generation_id="gen-1", cluster="ares", ttl_seconds=60, now=t0
    )
    queue.close_owner_session_lease(
        "session-closed", session_generation_id="gen-1", reason="client_close", now=t0
    )
    # Not due: a different cluster.
    queue.touch_owner_session_lease(
        "session-other-cluster",
        session_generation_id="gen-1",
        cluster="not-ares",
        ttl_seconds=60,
        now=t0,
    )

    now = t0 + timedelta(minutes=5)
    due = queue.due_expired_owner_session_leases(cluster="ares", now=now)

    assert [lease.owner_session_id for lease in due] == ["session-due"]


def test_due_expired_leases_is_sorted_and_bounded(tmp_path: Path) -> None:
    queue = _queue(tmp_path)
    t0 = datetime(2026, 1, 1, tzinfo=UTC)
    for name in ("session-c", "session-a", "session-b"):
        queue.touch_owner_session_lease(
            name, session_generation_id="gen-1", cluster="ares", ttl_seconds=1, now=t0
        )

    due = queue.due_expired_owner_session_leases(
        cluster="ares", now=t0 + timedelta(seconds=10), limit=2
    )

    assert [lease.owner_session_id for lease in due] == ["session-a", "session-b"]


# --- worker-side sweep -------------------------------------------------


def _worker(queue: ClioCoreQueue, core_dir: Path) -> EndpointWorker:
    """A minimal duck-typed stand-in: the sweep only reads ``.queue``/``.settings``."""
    return cast(
        EndpointWorker,
        SimpleNamespace(queue=queue, settings=RelaySettings(core_dir=core_dir)),
    )


def _submittable_job(
    *, owner_session_id: str, session_generation_id: str, idempotency_key: str
) -> RelayJob:
    return RelayJob(
        cluster="ares",
        kind=JobKind.JARVIS,
        spec=JarvisRunSpec(command=["true"]),
        idempotency_key=idempotency_key,
        metadata={
            "owner": "clio-relay",
            "owner_session_id": owner_session_id,
            "owner_session_generation_id": session_generation_id,
        },
    )


def test_sweep_skips_generation_already_moved_on_but_reaps_the_lease(tmp_path: Path) -> None:
    import clio_relay.endpoint_owner_session_sweep as sweep_module

    queue = _queue(tmp_path)
    _start_generation(queue, owner_session_id="desktop-session-1", generation_id="generation-1")
    t0 = datetime(2026, 1, 1, tzinfo=UTC)
    queue.touch_owner_session_lease(
        "desktop-session-1",
        session_generation_id="generation-1",
        cluster="ares",
        ttl_seconds=1,
        now=t0,
    )
    # A concurrent client-driven teardown already closed this generation.
    queue.set_owner_session_closing("desktop-session-1", session_generation_id="generation-1")
    queue.set_owner_session_closed(
        "desktop-session-1", session_generation_id="generation-1", residual_resource_ids=[]
    )

    called = False

    def _unexpected_teardown(*_args: object, **_kwargs: object) -> None:
        nonlocal called
        called = True
        raise AssertionError(
            "execute_owned_session_teardown must not run for a moved-on generation"
        )

    original = sweep_module.execute_owned_session_teardown
    sweep_module.execute_owned_session_teardown = _unexpected_teardown  # type: ignore[assignment]
    try:
        worker = _worker(queue, tmp_path / "core")
        swept = sweep_module.sweep_expired_owner_session_leases(worker, cluster="ares")
    finally:
        sweep_module.execute_owned_session_teardown = original  # type: ignore[assignment]

    assert called is False
    assert swept == 1
    lease = queue.owner_session_lease_status(
        "desktop-session-1", session_generation_id="generation-1"
    )
    assert lease is not None
    assert lease.status == "expired"


def test_sweep_closes_admission_and_marks_lease_when_teardown_succeeds(tmp_path: Path) -> None:
    import clio_relay.endpoint_owner_session_sweep as sweep_module

    queue = _queue(tmp_path)
    _start_generation(queue, owner_session_id="desktop-session-1", generation_id="generation-1")
    t0 = datetime(2026, 1, 1, tzinfo=UTC)
    queue.touch_owner_session_lease(
        "desktop-session-1",
        session_generation_id="generation-1",
        cluster="ares",
        ttl_seconds=1,
        now=t0,
    )

    teardown_calls: list[object] = []

    def _fake_teardown(request: object, **_kwargs: object) -> None:
        teardown_calls.append(request)

    original = sweep_module.execute_owned_session_teardown
    sweep_module.execute_owned_session_teardown = _fake_teardown  # type: ignore[assignment]
    try:
        worker = _worker(queue, tmp_path / "core")
        swept = sweep_module.sweep_expired_owner_session_leases(worker, cluster="ares")
    finally:
        sweep_module.execute_owned_session_teardown = original  # type: ignore[assignment]

    assert swept == 1
    assert len(teardown_calls) == 1
    admission = queue.owner_session_generation_status(
        "desktop-session-1", session_generation_id="generation-1"
    )
    assert admission["closed"] is True
    lease = queue.owner_session_lease_status(
        "desktop-session-1", session_generation_id="generation-1"
    )
    assert lease is not None
    assert lease.status == "expired"
    assert lease.close_reason == "lease_expired"
    assert lease.expired_with_running_jobs is False


def test_sweep_marks_expired_with_running_jobs_and_appends_typed_job_events(
    tmp_path: Path,
) -> None:
    import clio_relay.endpoint_owner_session_sweep as sweep_module

    queue = _queue(tmp_path)
    _start_generation(queue, owner_session_id="desktop-session-1", generation_id="generation-1")
    queue.touch_owner_session_lease(
        "desktop-session-1",
        session_generation_id="generation-1",
        cluster="ares",
        ttl_seconds=1,
        now=datetime(2026, 1, 1, tzinfo=UTC),
    )
    running = queue.submit_job(
        _submittable_job(
            owner_session_id="desktop-session-1",
            session_generation_id="generation-1",
            idempotency_key="sweep-job-1",
        )
    )
    # submit_job admits QUEUED; force the job + membership record into
    # RUNNING so it is discoverable via list_owner_session_jobs_page's
    # non-terminal filter, exactly like a real in-flight execution.
    queue.update_job_state(running.job_id, JobState.RUNNING, message="running")

    def _fake_teardown(*_args: object, **_kwargs: object) -> None:
        return None

    original = sweep_module.execute_owned_session_teardown
    sweep_module.execute_owned_session_teardown = _fake_teardown  # type: ignore[assignment]
    try:
        worker = _worker(queue, tmp_path / "core")
        swept = sweep_module.sweep_expired_owner_session_leases(worker, cluster="ares")
    finally:
        sweep_module.execute_owned_session_teardown = original  # type: ignore[assignment]

    assert swept == 1
    lease = queue.owner_session_lease_status(
        "desktop-session-1", session_generation_id="generation-1"
    )
    assert lease is not None
    assert lease.expired_with_running_jobs is True
    assert lease.running_job_ids_at_close == [running.job_id]
    # The job itself is untouched -- leases never kill jobs.
    assert queue.get_job(running.job_id).state == JobState.RUNNING

    events, _cursor = queue.read_event_page(running.job_id, next_seq=1, limit=100)
    typed = [event for event in events if event.event_type == "owner_session.lease_expired"]
    assert len(typed) == 1
    assert typed[0].payload["expired_with_running_jobs"] is True


def test_sweep_continues_past_one_sessions_teardown_failure(tmp_path: Path) -> None:
    """One poisoned session must never wedge the whole worker cycle (#238 doctrine)."""
    import clio_relay.endpoint_owner_session_sweep as sweep_module

    queue = _queue(tmp_path)
    _start_generation(queue, owner_session_id="desktop-session-1", generation_id="generation-1")
    _start_generation(queue, owner_session_id="desktop-session-2", generation_id="generation-1")
    t0 = datetime(2026, 1, 1, tzinfo=UTC)
    for session_id in ("desktop-session-1", "desktop-session-2"):
        queue.touch_owner_session_lease(
            session_id,
            session_generation_id="generation-1",
            cluster="ares",
            ttl_seconds=1,
            now=t0,
        )

    def _flaky_teardown(request: object, **_kwargs: object) -> None:
        if getattr(request, "session_id", None) == "desktop-session-1":
            raise RelayError("simulated teardown failure")

    original = sweep_module.execute_owned_session_teardown
    sweep_module.execute_owned_session_teardown = _flaky_teardown  # type: ignore[assignment]
    try:
        worker = _worker(queue, tmp_path / "core")
        swept = sweep_module.sweep_expired_owner_session_leases(worker, cluster="ares")
    finally:
        sweep_module.execute_owned_session_teardown = original  # type: ignore[assignment]

    # execute_owned_session_teardown raising RelayError is caught INSIDE
    # _sweep_one_owner_session_lease (non-fatal to that session's own sweep,
    # see the module docstring) -- both sessions still finish sweeping.
    assert swept == 2
    for session_id in ("desktop-session-1", "desktop-session-2"):
        lease = queue.owner_session_lease_status(session_id, session_generation_id="generation-1")
        assert lease is not None
        assert lease.status == "expired"


# --- clean client-driven teardown closes the lease distinctly ----------


def test_client_teardown_closes_the_lease_as_client_close_not_expired(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The two typed close paths (#277 design point 3) are never conflated."""
    import clio_relay.cli_session_owned as cli_session_owned
    from clio_relay.session_lifecycle import OwnedSessionTeardownRequest

    core_dir = tmp_path / "core"
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(core_dir))
    queue = ClioCoreQueue(core_dir)
    queue.initialize()
    _start_generation(queue, owner_session_id="desktop-session-1", generation_id="generation-1")
    queue.touch_owner_session_lease(
        "desktop-session-1",
        session_generation_id="generation-1",
        cluster="ares",
        ttl_seconds=1800,
    )

    request = OwnedSessionTeardownRequest(
        cluster="ares",
        session_id="desktop-session-1",
        expected_session_generation_id="generation-1",
        expected_cleanup_operation_id="cleanup_test",
        stop_worker=False,
        cancel_jobs=False,
        cancel_scheduler_jobs=False,
    )
    cli_session_owned._close_owner_session_lease_on_client_teardown(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        request
    )

    lease = queue.owner_session_lease_status(
        "desktop-session-1", session_generation_id="generation-1"
    )
    assert lease is not None
    assert lease.status == "closed"
    assert lease.close_reason == "client_close"
    # Distinguishable from the sweep's own typed path: never "expired".
    assert lease.status != "expired"


def test_client_teardown_lease_close_race_with_a_concurrent_sweep_is_non_fatal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A client teardown racing an already-completed sweep must not crash."""
    import clio_relay.cli_session_owned as cli_session_owned
    from clio_relay.session_lifecycle import OwnedSessionTeardownRequest

    core_dir = tmp_path / "core"
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(core_dir))
    queue = ClioCoreQueue(core_dir)
    queue.initialize()
    _start_generation(queue, owner_session_id="desktop-session-1", generation_id="generation-1")
    queue.touch_owner_session_lease(
        "desktop-session-1",
        session_generation_id="generation-1",
        cluster="ares",
        ttl_seconds=1800,
    )
    # The sweep got there first and already closed it as expired.
    queue.close_owner_session_lease(
        "desktop-session-1",
        session_generation_id="generation-1",
        reason="lease_expired",
    )

    request = OwnedSessionTeardownRequest(
        cluster="ares",
        session_id="desktop-session-1",
        expected_session_generation_id="generation-1",
        expected_cleanup_operation_id="cleanup_test",
        stop_worker=False,
        cancel_jobs=False,
        cancel_scheduler_jobs=False,
    )
    # Must not raise -- see the function's own docstring.
    cli_session_owned._close_owner_session_lease_on_client_teardown(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        request
    )

    lease = queue.owner_session_lease_status(
        "desktop-session-1", session_generation_id="generation-1"
    )
    assert lease is not None
    assert lease.status == "expired"  # the sweep's typed reason was not overwritten


# --- worker-cycle wiring: run_once joins the SAME sweep, every cycle ---


def test_run_once_sweeps_owner_session_leases_every_cycle(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The sweep joins run_once's existing per-cycle machinery -- including
    the FIRST cycle after a worker restart (#238/#240's "replay leases
    before accepting work" ask, for the owned-session-lease dimension)."""
    import clio_relay.endpoint_serve_loop as endpoint_serve_loop

    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    worker = EndpointWorker(
        role=EndpointRole.WORKER,
        settings=settings,
        cluster="configured-target",
        queue=queue,
    )

    calls: list[tuple[object, str]] = []
    original = endpoint_serve_loop.sweep_expired_owner_session_leases

    def _recording_sweep(recorded_worker: object, *, cluster: str) -> int:
        calls.append((recorded_worker, cluster))
        return original(recorded_worker, cluster=cluster)  # type: ignore[arg-type]

    monkeypatch.setattr(endpoint_serve_loop, "sweep_expired_owner_session_leases", _recording_sweep)

    worker.run_once()

    assert calls == [(worker, "configured-target")]


# --- session recovery-status lease projection (attach typed-reason path) --


def test_lease_status_projection_returns_none_when_absent(tmp_path: Path) -> None:
    from clio_relay.session_recovery_lease_projection import (
        owner_session_lease_status_projection,
    )

    core_dir = tmp_path / "core"
    ClioCoreQueue(core_dir).initialize()

    assert (
        owner_session_lease_status_projection(
            core_dir=core_dir,
            session_id="desktop-session-1",
            session_generation_id="generation-1",
        )
        is None
    )


def test_lease_status_projection_returns_the_lease_as_a_plain_dict(tmp_path: Path) -> None:
    from clio_relay.session_recovery_lease_projection import (
        owner_session_lease_status_projection,
    )

    core_dir = tmp_path / "core"
    queue = ClioCoreQueue(core_dir)
    queue.initialize()
    queue.touch_owner_session_lease(
        "desktop-session-1",
        session_generation_id="generation-1",
        cluster="ares",
        ttl_seconds=1800,
    )
    queue.close_owner_session_lease(
        "desktop-session-1",
        session_generation_id="generation-1",
        reason="lease_expired",
        running_job_ids=["job_x"],
    )

    projection = owner_session_lease_status_projection(
        core_dir=core_dir,
        session_id="desktop-session-1",
        session_generation_id="generation-1",
    )

    assert projection is not None
    assert projection["status"] == "expired"
    assert projection["close_reason"] == "lease_expired"
    assert projection["expired_with_running_jobs"] is True
    assert projection["running_job_ids_at_close"] == ["job_x"]


def test_cli_recovery_status_attaches_the_lease_projection(tmp_path: Path) -> None:
    """The wrapper that reaches the single-dial bootstrap script path."""
    import clio_relay.cli_session_owned as cli_session_owned
    from clio_relay.session_lifecycle import OwnedSessionRecoveryStatus

    core_dir = tmp_path / "core"
    queue = ClioCoreQueue(core_dir)
    queue.initialize()
    queue.touch_owner_session_lease(
        "desktop-session-1",
        session_generation_id="generation-1",
        cluster="ares",
        ttl_seconds=1800,
    )
    queue.close_owner_session_lease(
        "desktop-session-1",
        session_generation_id="generation-1",
        reason="lease_expired",
        running_job_ids=["job_x"],
    )
    bare_status = OwnedSessionRecoveryStatus(
        cluster="ares",
        session_id="desktop-session-1",
        session_generation_id="generation-1",
    )
    assert bare_status.owner_session_lease_status is None

    projected = cli_session_owned._with_owner_session_lease_projection(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        bare_status, core_dir=core_dir
    )

    assert projected.owner_session_lease_status is not None
    assert projected.owner_session_lease_status["status"] == "expired"
    # Every other field is untouched -- this is a pure additive projection.
    assert projected.model_dump(exclude={"owner_session_lease_status"}) == bare_status.model_dump(
        exclude={"owner_session_lease_status"}
    )


def test_cli_recovery_status_projection_is_a_no_op_when_generation_is_unknown(
    tmp_path: Path,
) -> None:
    import clio_relay.cli_session_owned as cli_session_owned
    from clio_relay.session_lifecycle import OwnedSessionRecoveryStatus

    bare_status = OwnedSessionRecoveryStatus(cluster="ares", session_id="desktop-session-1")

    projected = cli_session_owned._with_owner_session_lease_projection(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        bare_status, core_dir=tmp_path / "core"
    )

    assert projected == bare_status
