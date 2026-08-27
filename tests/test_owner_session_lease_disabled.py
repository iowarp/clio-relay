"""The disabled (ttl 0) owner-session lease -- the DEFAULT posture.

Owner ruling (2026-08-27, direct): a timeout must never destroy a live
session by default. ``ttl_seconds=0`` means the lease exists as a uniform,
self-describing record but can never become due; an explicit clean teardown
is the only terminator. Bounded self-clean is an opt-in deployment choice
(TTL >= 30); the 1-29s band is refused as a reap-storm footgun.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from pydantic import ValidationError

from clio_relay.config import RelaySettings
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.models_owner_session_lease import (
    DEFAULT_OWNER_SESSION_LEASE_TTL_SECONDS,
    OwnerSessionLease,
)

T0 = datetime(2026, 1, 1, tzinfo=UTC)
DECADE_LATER = T0 + timedelta(days=3650)


def _queue(tmp_path: Path) -> ClioCoreQueue:
    queue = ClioCoreQueue(tmp_path / "core")
    queue.initialize()
    return queue


def _touch(queue: ClioCoreQueue, *, ttl_seconds: int, now: datetime) -> OwnerSessionLease:
    return queue.touch_owner_session_lease(
        "desktop-session-1",
        session_generation_id="generation-1",
        cluster="ares",
        ttl_seconds=ttl_seconds,
        now=now,
    )


def test_the_default_ttl_is_disabled() -> None:
    assert DEFAULT_OWNER_SESSION_LEASE_TTL_SECONDS == 0
    assert RelaySettings().owner_session_lease_ttl_seconds == 0


def test_a_disabled_lease_is_never_due_no_matter_how_quiet(tmp_path: Path) -> None:
    queue = _queue(tmp_path)
    lease = _touch(queue, ttl_seconds=0, now=T0)

    assert lease.status == "open"
    assert lease.ttl_seconds == 0
    assert lease.is_due(now=DECADE_LATER) is False
    assert queue.due_expired_owner_session_leases(cluster="ares", now=DECADE_LATER) == []


def test_an_opted_in_sibling_still_expires_beside_a_disabled_lease(tmp_path: Path) -> None:
    queue = _queue(tmp_path)
    _touch(queue, ttl_seconds=0, now=T0)
    queue.touch_owner_session_lease(
        "desktop-session-2",
        session_generation_id="generation-2",
        cluster="ares",
        ttl_seconds=30,
        now=T0,
    )

    due = queue.due_expired_owner_session_leases(cluster="ares", now=DECADE_LATER)

    assert [lease.owner_session_id for lease in due] == ["desktop-session-2"]


def test_renewing_a_disabled_lease_never_rewrites_the_record(tmp_path: Path) -> None:
    queue = _queue(tmp_path)
    first = _touch(queue, ttl_seconds=0, now=T0)

    renewed = _touch(queue, ttl_seconds=0, now=DECADE_LATER)

    # Freshness is meaningless for a lease that can never be due; the
    # fsync'd rewrite every authenticated request would pay is skipped.
    assert renewed.last_seen_at == first.last_seen_at == T0


def test_clean_teardown_is_still_the_one_terminator_for_a_disabled_lease(
    tmp_path: Path,
) -> None:
    queue = _queue(tmp_path)
    _touch(queue, ttl_seconds=0, now=T0)

    queue.close_owner_session_lease(
        "desktop-session-1",
        session_generation_id="generation-1",
        reason="client_close",
        now=DECADE_LATER,
    )

    status = queue.owner_session_lease_status("desktop-session-1")
    assert status is not None
    assert status.status == "closed"
    assert status.close_reason == "client_close"


def _lease_model(ttl_seconds: int) -> OwnerSessionLease:
    return OwnerSessionLease(
        owner_session_id="desktop-session-1",
        session_generation_id="generation-1",
        cluster="ares",
        opened_at=T0,
        last_seen_at=T0,
        ttl_seconds=ttl_seconds,
    )


def test_the_lease_model_accepts_zero_and_refuses_negatives() -> None:
    assert _lease_model(0).ttl_seconds == 0
    with pytest.raises(ValidationError):
        _lease_model(-1)


@pytest.mark.parametrize("footgun", [1, 15, 29])
def test_settings_refuse_the_reap_storm_footgun_band(footgun: int) -> None:
    with pytest.raises(ValidationError, match="0 .*disabled.* or at least 30"):
        RelaySettings(owner_session_lease_ttl_seconds=footgun)


@pytest.mark.parametrize("legal", [0, 30, 1_800, 86_400])
def test_settings_accept_disabled_and_the_opt_in_range(legal: int) -> None:
    assert RelaySettings(owner_session_lease_ttl_seconds=legal).owner_session_lease_ttl_seconds == (
        legal
    )


def test_env_zero_reaches_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CLIO_RELAY_OWNER_SESSION_LEASE_TTL_SECONDS", "0")
    assert RelaySettings.from_env().owner_session_lease_ttl_seconds == 0
