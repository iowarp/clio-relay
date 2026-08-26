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
from clio_relay.session_lifecycle import OwnedSessionTeardownRequest


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
    """Past the debounce window (a quarter-TTL, MEDIUM 5): a real renewal write."""
    queue = _queue(tmp_path)
    opened_at = datetime(2026, 1, 1, tzinfo=UTC)
    later = opened_at + timedelta(minutes=20)  # > ttl_seconds/4 == 450s

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


def test_touch_debounces_a_renewal_within_a_quarter_ttl(tmp_path: Path) -> None:
    """MEDIUM 5: skip the fsync'd write when it would not change due-ness."""
    queue = _queue(tmp_path)
    opened_at = datetime(2026, 1, 1, tzinfo=UTC)
    soon = opened_at + timedelta(minutes=5)  # < ttl_seconds/4 == 450s

    queue.touch_owner_session_lease(
        "desktop-session-1",
        session_generation_id="generation-1",
        cluster="ares",
        ttl_seconds=1800,
        now=opened_at,
    )
    debounced = queue.touch_owner_session_lease(
        "desktop-session-1",
        session_generation_id="generation-1",
        cluster="ares",
        ttl_seconds=1800,
        now=soon,
    )

    assert debounced.last_seen_at == opened_at  # unchanged -- no write happened
    assert debounced.status == "open"


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


# --- BLOCKER 3: compare-and-swap against a renewal landing mid-sweep ---


def test_close_with_a_stale_expected_last_seen_at_is_a_typed_no_op(tmp_path: Path) -> None:
    """The exact race the review proved: due-scan reads last_seen_at=T0,
    slow work happens, a renewal lands at T1, THEN close is attempted
    anchored to T0 -- must not overwrite the live renewal."""
    queue = _queue(tmp_path)
    t0 = datetime(2026, 1, 1, tzinfo=UTC)
    t1 = t0 + timedelta(hours=1)  # past the debounce window too

    queue.touch_owner_session_lease(
        "desktop-session-1",
        session_generation_id="generation-1",
        cluster="ares",
        ttl_seconds=1800,
        now=t0,
    )
    # The renewal that lands mid-sweep, AFTER the due-scan read t0.
    renewed = queue.touch_owner_session_lease(
        "desktop-session-1",
        session_generation_id="generation-1",
        cluster="ares",
        ttl_seconds=1800,
        now=t1,
    )
    assert renewed.last_seen_at == t1

    result = queue.close_owner_session_lease(
        "desktop-session-1",
        session_generation_id="generation-1",
        reason="lease_expired",
        expected_last_seen_at=t0,  # stale -- the due-scan's own read
    )

    assert result is not None
    assert result.status == "open"  # NOT closed -- the CAS refused
    assert result.last_seen_at == t1  # the live renewal, untouched


def test_close_with_a_matching_expected_last_seen_at_closes_normally(tmp_path: Path) -> None:
    """No race: the CAS anchor matches the current record, close proceeds."""
    queue = _queue(tmp_path)
    t0 = datetime(2026, 1, 1, tzinfo=UTC)

    queue.touch_owner_session_lease(
        "desktop-session-1",
        session_generation_id="generation-1",
        cluster="ares",
        ttl_seconds=1800,
        now=t0,
    )

    result = queue.close_owner_session_lease(
        "desktop-session-1",
        session_generation_id="generation-1",
        reason="lease_expired",
        expected_last_seen_at=t0,
    )

    assert result is not None
    assert result.status == "expired"


def test_sweep_aborts_before_teardown_when_renewed_after_quiesce(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """BLOCKER 3's second half: the sweep re-reads and re-checks is_due AFTER
    quiescing and BEFORE calling teardown (which would kill a live process).
    A renewal landing in exactly that window must abort before teardown."""
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

    teardown_calls: list[object] = []
    original_quiesce = type(queue).set_owner_session_closing

    def _quiesce_then_renew(self: ClioCoreQueue, *args: object, **kwargs: object) -> object:
        # Simulate a renewal landing in the narrow window between the
        # sweep's quiesce call and its post-quiesce re-check. A dynamic
        # passthrough wrapper genuinely cannot be typed more precisely than
        # this against the real method's exact signature.
        result = original_quiesce(self, *args, **kwargs)  # pyright: ignore[reportArgumentType]
        self.touch_owner_session_lease(
            "desktop-session-1",
            session_generation_id="generation-1",
            cluster="ares",
            ttl_seconds=1,
        )
        return result

    def _unexpected_teardown(*_args: object, **_kwargs: object) -> None:
        teardown_calls.append(_args)
        raise AssertionError("teardown must not run on a lease renewed after quiesce")

    monkeypatch.setattr(ClioCoreQueue, "set_owner_session_closing", _quiesce_then_renew)
    monkeypatch.setattr(sweep_module, "execute_owned_session_teardown", _unexpected_teardown)

    worker = _worker(queue, tmp_path / "core")
    swept = sweep_module.sweep_expired_owner_session_leases(worker, cluster="ares")

    assert teardown_calls == []
    assert swept == 0
    lease = queue.owner_session_lease_status(
        "desktop-session-1", session_generation_id="generation-1"
    )
    assert lease is not None
    assert lease.status == "open"  # the process was never touched


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


# --- BLOCKER 1: the sweep's own operation_id must survive teardown's ---
# --- internal re-quiesce call, using the REAL queue precondition check --


def test_a_fresh_operation_id_makes_the_real_requiesce_precondition_refuse(
    tmp_path: Path,
) -> None:
    """Root-cause proof, zero mocking: this is EXACTLY the mechanism BLOCKER 1
    named. ``execute_owned_session_teardown`` cannot run on this platform (it
    needs a real Linux systemd-contained session transaction), but the bug
    was never in that machinery -- it was in this ONE precondition check,
    which is 100% real code here. The sweep's own quiesce call mints
    ``cleanup_<uuidA>`` as the immutable intent; teardown's OWN internal
    re-quiesce call (this second call, replicating exactly what
    ``execute_owned_session_teardown`` does with a request's
    ``expected_cleanup_operation_id``) with a DIFFERENT fresh uuid refuses.
    """
    queue = _queue(tmp_path)
    _start_generation(queue, owner_session_id="desktop-session-1", generation_id="generation-1")

    sweep_intent = queue.set_owner_session_closing(
        "desktop-session-1",
        session_generation_id="generation-1",
        stop_worker=False,
        cancel_jobs=False,
        cancel_scheduler_jobs=False,
    )
    assert cast(str, sweep_intent["operation_id"]).startswith("cleanup_")

    with pytest.raises(QueueConflictError, match="operation changed during retry"):
        queue.set_owner_session_closing(
            "desktop-session-1",
            session_generation_id="generation-1",
            operation_id=f"cleanup_{'b' * 32}",  # a DIFFERENT fresh id -- the old bug
            stop_worker=False,
            cancel_jobs=False,
            cancel_scheduler_jobs=False,
        )


def test_reusing_the_quiesce_intents_operation_id_does_not_refuse(tmp_path: Path) -> None:
    """The fix, same real precondition check: reusing the FIRST call's own
    ``operation_id`` (what ``execute_owned_session_teardown``'s internal
    re-quiesce sees when the sweep passes it through
    ``expected_cleanup_operation_id``) is accepted as the identical retry it
    actually is."""
    queue = _queue(tmp_path)
    _start_generation(queue, owner_session_id="desktop-session-1", generation_id="generation-1")

    sweep_intent = queue.set_owner_session_closing(
        "desktop-session-1",
        session_generation_id="generation-1",
        stop_worker=False,
        cancel_jobs=False,
        cancel_scheduler_jobs=False,
    )
    operation_id = cast(str, sweep_intent["operation_id"])

    replayed_intent = queue.set_owner_session_closing(
        "desktop-session-1",
        session_generation_id="generation-1",
        operation_id=operation_id,
        stop_worker=False,
        cancel_jobs=False,
        cancel_scheduler_jobs=False,
    )
    assert replayed_intent == sweep_intent


def test_sweep_reuses_its_own_quiesce_operation_id_for_teardown(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Integration-level proof: a fake ``execute_owned_session_teardown`` that
    faithfully replicates the ONE real line that matters (teardown's own
    internal ``queue.set_owner_session_closing`` re-quiesce call, using the
    REAL queue) must NOT raise when driven by the actual sweep -- proving
    the sweep threads its own quiesce call's ``operation_id`` through
    ``OwnedSessionTeardownRequest.expected_cleanup_operation_id`` correctly
    (BLOCKER 1). Before the fix this raised ``QueueConflictError`` on every
    single sweep of every session, unconditionally.
    """
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

    def _faithful_fake_teardown(request: OwnedSessionTeardownRequest, **_kwargs: object) -> None:
        # Exactly what execute_owned_session_teardown's own body does with
        # its request, using the REAL queue -- see session_cleanup_execution.py.
        queue.set_owner_session_closing(
            request.session_id,
            session_generation_id=request.expected_session_generation_id,
            operation_id=request.expected_cleanup_operation_id,
            stop_worker=request.stop_worker,
            cancel_jobs=request.cancel_jobs,
            cancel_scheduler_jobs=request.cancel_scheduler_jobs,
        )

    monkeypatch.setattr(sweep_module, "execute_owned_session_teardown", _faithful_fake_teardown)

    worker = _worker(queue, tmp_path / "core")
    swept = sweep_module.sweep_expired_owner_session_leases(worker, cluster="ares")

    assert swept == 1
    admission = queue.owner_session_generation_status(
        "desktop-session-1", session_generation_id="generation-1"
    )
    assert admission["closed"] is True
    lease = queue.owner_session_lease_status(
        "desktop-session-1", session_generation_id="generation-1"
    )
    assert lease is not None
    assert lease.status == "expired"
    assert lease.sweep_failure_count == 0  # never hit the failure path at all


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


# --- BLOCKER 2: honor a RECORDED cleanup intent; bound retries ---------


def test_sweep_honors_a_recorded_cleanup_intent_instead_of_hardcoding(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A client that ran ``session quiesce-intake --cancel-jobs`` before
    crashing already recorded ``cancel_jobs=True`` durably. The sweep's own
    hardcoded all-``False`` policy would conflict with that -- proven here
    with the REAL ``set_owner_session_closing`` precondition check, not a
    mock -- so the sweep must read and reuse the recorded intent instead.
    """
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
    # The client's own `session quiesce-intake --cancel-jobs` call, before it
    # crashed -- durably recorded with cancel_jobs=True.
    recorded_intent = queue.set_owner_session_closing(
        "desktop-session-1",
        session_generation_id="generation-1",
        stop_worker=False,
        cancel_jobs=True,
        cancel_scheduler_jobs=False,
    )

    teardown_calls: list[OwnedSessionTeardownRequest] = []

    def _recording_teardown(request: OwnedSessionTeardownRequest, **_kwargs: object) -> None:
        teardown_calls.append(request)
        # Faithful fake: replicate the real internal re-quiesce call so an
        # unresolved policy/operation_id mismatch still surfaces here too.
        queue.set_owner_session_closing(
            request.session_id,
            session_generation_id=request.expected_session_generation_id,
            operation_id=request.expected_cleanup_operation_id,
            stop_worker=request.stop_worker,
            cancel_jobs=request.cancel_jobs,
            cancel_scheduler_jobs=request.cancel_scheduler_jobs,
        )

    monkeypatch.setattr(sweep_module, "execute_owned_session_teardown", _recording_teardown)

    worker = _worker(queue, tmp_path / "core")
    swept = sweep_module.sweep_expired_owner_session_leases(worker, cluster="ares")

    assert swept == 1
    assert len(teardown_calls) == 1
    request = teardown_calls[0]
    assert request.cancel_jobs is True  # type: ignore[attr-defined]
    assert request.expected_cleanup_operation_id == recorded_intent["operation_id"]  # type: ignore[attr-defined]
    lease = queue.owner_session_lease_status(
        "desktop-session-1", session_generation_id="generation-1"
    )
    assert lease is not None
    assert lease.status == "expired"
    assert lease.sweep_failure_count == 0


def test_sweep_defaults_to_conservative_policy_when_no_intent_is_recorded(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The ordinary crash case (no client ever ran quiesce-intake): the
    sweep's own all-``False`` default, not an inherited one."""
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

    teardown_calls: list[OwnedSessionTeardownRequest] = []

    def _recording_teardown(request: OwnedSessionTeardownRequest, **_kwargs: object) -> None:
        teardown_calls.append(request)

    monkeypatch.setattr(sweep_module, "execute_owned_session_teardown", _recording_teardown)

    worker = _worker(queue, tmp_path / "core")
    sweep_module.sweep_expired_owner_session_leases(worker, cluster="ares")

    assert len(teardown_calls) == 1
    request = teardown_calls[0]
    assert request.stop_worker is False
    assert request.cancel_jobs is False
    assert request.cancel_scheduler_jobs is False


def test_sweep_bounds_retries_and_quarantines_instead_of_spinning_forever(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """BLOCKER 2's second, independent belt: ANY per-lease failure (not just
    the recorded-intent-conflict scenario) stops retrying after
    ``MAX_OWNER_SESSION_SWEEP_ATTEMPTS`` instead of spinning a fresh
    traceback every cycle forever (proven over N cycles, not asserted by
    inspection)."""
    import clio_relay.endpoint_owner_session_sweep as sweep_module
    from clio_relay.models_owner_session_lease import MAX_OWNER_SESSION_SWEEP_ATTEMPTS

    queue = _queue(tmp_path)
    _start_generation(queue, owner_session_id="desktop-session-1", generation_id="generation-1")
    queue.touch_owner_session_lease(
        "desktop-session-1",
        session_generation_id="generation-1",
        cluster="ares",
        ttl_seconds=1,
        now=datetime(2026, 1, 1, tzinfo=UTC),
    )

    def _always_broken_teardown(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("simulated unrecoverable per-attempt failure")

    monkeypatch.setattr(sweep_module, "execute_owned_session_teardown", _always_broken_teardown)
    # set_owner_session_closing itself is real and succeeds every time; the
    # failure is forced to occur AFTER it, inside execute_owned_session_
    # teardown, so each cycle's quiesce is a real idempotent no-op retry.

    worker = _worker(queue, tmp_path / "core")
    for cycle in range(MAX_OWNER_SESSION_SWEEP_ATTEMPTS):
        swept = sweep_module.sweep_expired_owner_session_leases(worker, cluster="ares")
        lease = queue.owner_session_lease_status(
            "desktop-session-1", session_generation_id="generation-1"
        )
        assert lease is not None
        if cycle + 1 < MAX_OWNER_SESSION_SWEEP_ATTEMPTS:
            assert swept == 0
            assert lease.status == "open"
            assert lease.sweep_failure_count == cycle + 1
        else:
            assert lease.status == "quarantined"
            assert lease.close_reason == "expiry_quarantined"
            assert "simulated unrecoverable per-attempt failure" in (lease.last_sweep_error or "")

    # A quarantined lease is terminal: the due-scan never selects it again,
    # so a FURTHER cycle changes nothing -- proof the spin actually stopped,
    # not just that the count capped out once.
    due_before = queue.due_expired_owner_session_leases(cluster="ares")
    assert due_before == []
    swept_again = sweep_module.sweep_expired_owner_session_leases(worker, cluster="ares")
    assert swept_again == 0
    final_lease = queue.owner_session_lease_status(
        "desktop-session-1", session_generation_id="generation-1"
    )
    assert final_lease is not None
    assert final_lease.status == "quarantined"
    assert final_lease.sweep_failure_count == MAX_OWNER_SESSION_SWEEP_ATTEMPTS


# --- clean client-driven teardown closes the lease distinctly ----------


def test_client_teardown_closes_the_lease_as_client_close_not_expired(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The two typed close paths (#277 design point 3) are never conflated."""
    import clio_relay.cli_session_owned as cli_session_owned

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


# --- MEDIUM 6: wall-clock-vs-monotonic divergence guards a mass-expiry -


def test_clock_jump_detector_ignores_a_slow_cycle(monkeypatch: pytest.MonkeyPatch) -> None:
    """A legitimately slow cycle (a long-running job between polls) advances
    BOTH clocks together and must never trip the detector."""
    import clio_relay.endpoint_owner_session_sweep as sweep_module

    cluster = "clock-slow-cycle"
    sweep_module._LAST_SWEEP_CLOCKS.pop(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        cluster, None
    )
    wall = [1_000.0]
    mono = [1_000.0]
    monkeypatch.setattr(sweep_module.time, "time", lambda: wall[0])
    monkeypatch.setattr(sweep_module.time, "monotonic", lambda: mono[0])

    detect = sweep_module._clock_jump_detected  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    assert detect(cluster) is False
    wall[0] += 900.0  # a 15-minute job ran between polls...
    mono[0] += 900.0  # ...and the monotonic clock agrees.

    assert detect(cluster) is False


def test_clock_jump_detector_catches_a_forward_wall_clock_jump(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A genuine wall-clock jump (NTP correction, manual date change) diverges
    the wall clock from the monotonic clock -- proven: 0 due -> would-be-12
    due becomes 0 swept because the whole cycle is skipped."""
    import clio_relay.endpoint_owner_session_sweep as sweep_module

    cluster = "clock-jump"
    sweep_module._LAST_SWEEP_CLOCKS.pop(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        cluster, None
    )
    wall = [1_000.0]
    mono = [1_000.0]
    monkeypatch.setattr(sweep_module.time, "time", lambda: wall[0])
    monkeypatch.setattr(sweep_module.time, "monotonic", lambda: mono[0])

    detect = sweep_module._clock_jump_detected  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    assert detect(cluster) is False
    wall[0] += 1_801.0  # a TTL(1800s)+1s FORWARD wall-clock jump...
    mono[0] += 2.0  # ...but only ~2s of real time actually passed.

    assert detect(cluster) is True


def test_sweep_skips_the_whole_cycle_on_a_detected_clock_jump(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """End-to-end: a detected jump means the due-scan itself never runs --
    proven by a due lease that is NOT reaped despite being (falsely) due."""
    import clio_relay.endpoint_owner_session_sweep as sweep_module

    cluster = "clock-jump-e2e"
    sweep_module._LAST_SWEEP_CLOCKS.pop(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        cluster, None
    )
    queue = _queue(tmp_path)
    _start_generation(queue, owner_session_id="desktop-session-1", generation_id="generation-1")
    queue.touch_owner_session_lease(
        "desktop-session-1",
        session_generation_id="generation-1",
        cluster=cluster,
        ttl_seconds=1800,
        now=datetime(2026, 1, 1, tzinfo=UTC),
    )

    def _unexpected_teardown(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("teardown must not run on a clock-jumped cycle")

    monkeypatch.setattr(sweep_module, "execute_owned_session_teardown", _unexpected_teardown)
    wall = [1_000.0]
    mono = [1_000.0]
    monkeypatch.setattr(sweep_module.time, "time", lambda: wall[0])
    monkeypatch.setattr(sweep_module.time, "monotonic", lambda: mono[0])

    worker = _worker(queue, tmp_path / "core")
    assert sweep_module.sweep_expired_owner_session_leases(worker, cluster=cluster) == 0
    wall[0] += 1_801.0
    mono[0] += 2.0
    swept = sweep_module.sweep_expired_owner_session_leases(worker, cluster=cluster)

    assert swept == 0
    lease = queue.owner_session_lease_status(
        "desktop-session-1", session_generation_id="generation-1"
    )
    assert lease is not None
    assert lease.status == "open"  # untouched -- the whole cycle was skipped


# --- MEDIUM 7: due-scan/prune containment; nothing kills the worker loop --


def test_sweep_degrades_when_the_due_scan_itself_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A storage-layer safety-bound refusal (or any other scan failure) must
    degrade this ONE cycle, never propagate into the worker's own loop."""
    import clio_relay.endpoint_owner_session_sweep as sweep_module

    queue = _queue(tmp_path)

    def _broken_scan(*_args: object, **_kwargs: object) -> list[object]:
        raise QueueConflictError("simulated directory-scan safety bound")

    monkeypatch.setattr(type(queue), "due_expired_owner_session_leases", _broken_scan)

    worker = _worker(queue, tmp_path / "core")
    swept = sweep_module.sweep_expired_owner_session_leases(worker, cluster="ares")

    assert swept == 0


def test_sweep_degrades_when_pruning_fails(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Pruning is best-effort housekeeping: its own failure must not turn a
    cycle that otherwise made progress into one that raises."""
    import clio_relay.endpoint_owner_session_sweep as sweep_module

    queue = _queue(tmp_path)

    def _broken_prune(*_args: object, **_kwargs: object) -> int:
        raise QueueConflictError("simulated prune failure")

    monkeypatch.setattr(type(queue), "prune_terminal_owner_session_leases", _broken_prune)

    worker = _worker(queue, tmp_path / "core")
    # Must not raise, even with nothing due to sweep.
    swept = sweep_module.sweep_expired_owner_session_leases(worker, cluster="ares")

    assert swept == 0


def test_prune_removes_only_terminal_leases_past_the_retention_window(tmp_path: Path) -> None:
    queue = _queue(tmp_path)
    t0 = datetime(2026, 1, 1, tzinfo=UTC)

    # Terminal, old enough -- pruned.
    queue.touch_owner_session_lease(
        "session-old-closed", session_generation_id="gen-1", cluster="ares", ttl_seconds=60, now=t0
    )
    queue.close_owner_session_lease(
        "session-old-closed", session_generation_id="gen-1", reason="client_close", now=t0
    )
    # Terminal, but too recent -- kept.
    queue.touch_owner_session_lease(
        "session-fresh-closed",
        session_generation_id="gen-1",
        cluster="ares",
        ttl_seconds=60,
        now=t0,
    )
    queue.close_owner_session_lease(
        "session-fresh-closed",
        session_generation_id="gen-1",
        reason="client_close",
        now=t0 + timedelta(hours=23),
    )
    # Still open -- never pruned regardless of age.
    queue.touch_owner_session_lease(
        "session-open", session_generation_id="gen-1", cluster="ares", ttl_seconds=60, now=t0
    )

    now = t0 + timedelta(hours=25)
    pruned = queue.prune_terminal_owner_session_leases(
        cluster="ares", older_than_seconds=86_400, now=now
    )

    assert pruned == 1
    assert (
        queue.owner_session_lease_status("session-old-closed", session_generation_id="gen-1")
        is None
    )
    assert (
        queue.owner_session_lease_status("session-fresh-closed", session_generation_id="gen-1")
        is not None
    )
    assert (
        queue.owner_session_lease_status("session-open", session_generation_id="gen-1") is not None
    )


def test_sweep_prunes_terminal_leases_once_per_cycle(tmp_path: Path) -> None:
    """End-to-end proof the sweep actually calls pruning, not just that the
    queue primitive works in isolation."""
    import clio_relay.endpoint_owner_session_sweep as sweep_module

    queue = _queue(tmp_path)
    t0 = datetime(2026, 1, 1, tzinfo=UTC)
    queue.touch_owner_session_lease(
        "session-long-closed", session_generation_id="gen-1", cluster="ares", ttl_seconds=60, now=t0
    )
    queue.close_owner_session_lease(
        "session-long-closed", session_generation_id="gen-1", reason="client_close", now=t0
    )

    worker = _worker(queue, tmp_path / "core")
    sweep_module.sweep_expired_owner_session_leases(worker, cluster="ares")

    assert (
        queue.owner_session_lease_status("session-long-closed", session_generation_id="gen-1")
        is None
    )


# --- MINOR: the running-job-id page walk records honest truncation -----


def test_running_job_ids_helper_records_truncation_when_pages_are_cut_short(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Deterministic unit proof, independent of real page-size defaults:
    a fake page source that ALWAYS claims more is available forces the
    bounded walk to stop at its own page cap and report truncated=True."""
    import clio_relay.endpoint_owner_session_sweep as sweep_module

    queue = _queue(tmp_path)

    def _always_more_pages(
        self: ClioCoreQueue,
        owner_session_id: str,
        *,
        session_generation_id: str,
        cursor: str | None = None,
        include_terminal: bool = False,
        **_kwargs: object,
    ) -> tuple[list[RelayJob], str | None, int, int]:
        del self, owner_session_id, session_generation_id, include_terminal
        page_number = 0 if cursor is None else int(cursor)
        job = _submittable_job(
            owner_session_id="desktop-session-1",
            session_generation_id="generation-1",
            idempotency_key=f"truncation-job-{page_number}",
        ).model_copy(update={"job_id": f"job_truncation_{page_number}"})
        return [job], str(page_number + 1), 999, 1

    monkeypatch.setattr(ClioCoreQueue, "list_owner_session_jobs_page", _always_more_pages)

    worker = _worker(queue, tmp_path / "core")
    job_ids, truncated = sweep_module._owned_generation_running_job_ids(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        worker,
        owner_session_id="desktop-session-1",
        session_generation_id="generation-1",
    )

    assert truncated is True
    assert len(job_ids) == sweep_module.MAX_OWNER_SESSION_SWEEP_JOB_LISTING_PAGES


def test_running_job_ids_helper_reports_no_truncation_when_pages_are_exhausted(
    tmp_path: Path,
) -> None:
    queue = _queue(tmp_path)
    _start_generation(queue, owner_session_id="desktop-session-1", generation_id="generation-1")
    job = queue.submit_job(
        _submittable_job(
            owner_session_id="desktop-session-1",
            session_generation_id="generation-1",
            idempotency_key="untruncated-job",
        )
    )
    queue.update_job_state(job.job_id, JobState.RUNNING, message="running")

    import clio_relay.endpoint_owner_session_sweep as sweep_module

    worker = _worker(queue, tmp_path / "core")
    job_ids, truncated = sweep_module._owned_generation_running_job_ids(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        worker,
        owner_session_id="desktop-session-1",
        session_generation_id="generation-1",
    )

    assert truncated is False
    assert job_ids == [job.job_id]
