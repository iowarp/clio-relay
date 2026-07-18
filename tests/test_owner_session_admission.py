from __future__ import annotations

from contextlib import AbstractContextManager
from pathlib import Path
from typing import cast

import pytest

from clio_relay.cluster_config import ClusterDefinition
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.errors import QueueConflictError, RelayError
from clio_relay.models import GatewaySession
from clio_relay.owner_session_admission import (
    desktop_owner_session_admission_id,
    owner_session_gateway_admission,
)


def test_gateway_admission_reproduces_raw_owner_failure_then_mirrors_cluster_scope(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The shared boundary fixes the exact missing local admission seen live."""
    queue = ClioCoreQueue(tmp_path / "core")
    queue.initialize()
    definition = ClusterDefinition(name="test-cluster", ssh_host="relay.example.test")
    monkeypatch.setenv("CLIO_RELAY_CLI_MODE", "ssh")

    with pytest.raises(
        QueueConflictError,
        match="owner session generation has no active admission state",
    ):
        queue.create_gateway_session(
            GatewaySession(
                cluster=definition.name,
                name="raw-owner-session",
                metadata={
                    "owner": "clio-relay",
                    "owner_session_id": "desktop-session",
                    "owner_session_generation_id": "generation-1",
                },
            )
        )

    events: list[str] = []
    lock_held = False

    class RecordingLock(AbstractContextManager[object]):
        def __enter__(self) -> object:
            nonlocal lock_held
            assert lock_held is False
            lock_held = True
            events.append("enter")
            return self

        def __exit__(self, *_args: object) -> None:
            nonlocal lock_held
            assert lock_held is True
            lock_held = False
            events.append("exit")

    def lock_factory(*, cluster: str, session_id: str) -> RecordingLock:
        assert (cluster, session_id) == ("test-cluster", "desktop-session")
        return RecordingLock()

    def session_status(**_kwargs: object) -> dict[str, object]:
        assert lock_held is True
        events.append("session")
        return _live_session_status()

    def admission_status(**_kwargs: object) -> dict[str, object]:
        assert lock_held is True
        events.append("admission")
        return _open_admission_status()

    with owner_session_gateway_admission(
        queue=queue,
        definition=definition,
        cluster=definition.name,
        session_id="desktop-session",
        session_generation_id="generation-1",
        transition_lock_factory=lock_factory,
        session_status_reader=session_status,
        admission_status_reader=admission_status,
    ) as admission:
        assert lock_held is True
        events.append("gateway-write")
        gateway = queue.create_gateway_session(
            GatewaySession(
                cluster=definition.name,
                name="admitted-owner-session",
                metadata={
                    "owner": "clio-relay",
                    "owner_session_id": admission.owner_session_id,
                    "owner_session_generation_id": admission.owner_session_generation_id,
                    "owner_session_admission_id": admission.owner_session_admission_id,
                },
            )
        )

    assert events == [
        "enter",
        "session",
        "admission",
        "session",
        "admission",
        "gateway-write",
        "exit",
    ]
    assert gateway.metadata["owner_session_admission_id"].startswith("desktop_")
    assert gateway.metadata["owner_session_admission_id"] != "desktop-session"


def test_gateway_admission_teardown_race_fails_before_gateway_side_effects(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A cleanup intent appearing after mirroring wins before gateway creation."""
    queue = ClioCoreQueue(tmp_path / "core")
    queue.initialize()
    definition = ClusterDefinition(name="test-cluster", ssh_host="relay.example.test")
    monkeypatch.setenv("CLIO_RELAY_CLI_MODE", "ssh")
    admission_reads = 0

    def admission_status(**_kwargs: object) -> dict[str, object]:
        nonlocal admission_reads
        admission_reads += 1
        if admission_reads == 1:
            return _open_admission_status()
        return {
            **_open_admission_status(),
            "closing": True,
            "open": False,
            "closing_generation_id": "generation-1",
            "cleanup_intent": {
                "schema_version": "clio-relay.owner-session-cleanup-intent.v1",
                "operation_id": "cleanup_race",
                "owner_session_id": "desktop-session",
                "session_generation_id": "generation-1",
                "stop_worker": False,
                "cancel_jobs": False,
                "cancel_scheduler_jobs": False,
            },
        }

    def session_status(**_kwargs: object) -> dict[str, object]:
        return _live_session_status()

    entered = False
    with (
        pytest.raises(RelayError, match="is not open for gateway admission"),
        owner_session_gateway_admission(
            queue=queue,
            definition=definition,
            cluster=definition.name,
            session_id="desktop-session",
            session_generation_id="generation-1",
            session_status_reader=session_status,
            admission_status_reader=admission_status,
        ),
    ):
        entered = True

    assert entered is False
    assert admission_reads == 2
    assert queue.list_gateway_sessions() == []
    local_admission_id = desktop_owner_session_admission_id(
        cluster=definition.name,
        session_id="desktop-session",
    )
    reconciled = queue.owner_session_generation_status(
        local_admission_id,
        session_generation_id="generation-1",
    )
    assert reconciled["active_generation_id"] is None
    assert reconciled["closing"] is True
    assert reconciled["closed"] is True
    assert reconciled["open"] is False
    cleanup_intent = reconciled["cleanup_intent"]
    assert isinstance(cleanup_intent, dict)
    cleanup_intent = cast(dict[str, object], cleanup_intent)
    assert cleanup_intent.get("schema_version") == ("clio-relay.owner-session-cleanup-intent.v1")
    assert cleanup_intent.get("operation_id") == "cleanup_race"
    assert cleanup_intent.get("owner_session_id") == local_admission_id
    assert cleanup_intent.get("session_generation_id") == "generation-1"
    assert cleanup_intent.get("stop_worker") is False
    assert cleanup_intent.get("cancel_jobs") is False
    assert cleanup_intent.get("cancel_scheduler_jobs") is False
    assert isinstance(cleanup_intent.get("created_at"), str)
    reopened = queue.mirror_owner_session_generation_open(
        local_admission_id,
        session_generation_id="generation-2",
    )
    assert reopened["active_generation_id"] == "generation-2"
    assert reopened["open"] is True


def _live_session_status() -> dict[str, object]:
    return {
        "owner": "clio-relay",
        "session_id": "desktop-session",
        "session_generation_id": "generation-1",
        "running": True,
        "ownership_verified": True,
    }


def _open_admission_status() -> dict[str, object]:
    return {
        "schema_version": "clio-relay.owner-session-admission-status.v1",
        "owner_session_id": "desktop-session",
        "session_generation_id": "generation-1",
        "active_generation_id": "generation-1",
        "closing_generation_id": None,
        "active": True,
        "closing": False,
        "closed": False,
        "open": True,
        "cleanup_intent": None,
    }
