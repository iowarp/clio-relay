"""Unit tests for the durable owned-session record (iowarp/clio-relay#276 B1)."""

from __future__ import annotations

from pathlib import Path

import pytest

from clio_relay.owned_session_record import (
    OWNED_SESSION_REGISTRY_ENV,
    clear_owned_session_record,
    default_owned_session_record_path,
    load_owned_session_record,
    save_owned_session_record,
)


def test_load_before_any_save_returns_none(tmp_path: Path) -> None:
    path = tmp_path / "owned_sessions.json"
    assert load_owned_session_record("ares", path=path) is None


def test_save_then_load_round_trips_the_exact_record(tmp_path: Path) -> None:
    path = tmp_path / "owned_sessions.json"

    saved = save_owned_session_record(
        cluster="ares",
        session_id="desktop-session-1",
        session_generation_id="generation-1",
        remote_api_port=8765,
        path=path,
    )
    loaded = load_owned_session_record("ares", path=path)

    assert loaded == saved
    assert loaded is not None
    assert loaded.cluster == "ares"
    assert loaded.session_id == "desktop-session-1"
    assert loaded.session_generation_id == "generation-1"
    assert loaded.remote_api_port == 8765
    assert loaded.created_at


def test_a_new_bring_up_overwrites_the_prior_record_for_the_same_cluster(tmp_path: Path) -> None:
    path = tmp_path / "owned_sessions.json"
    save_owned_session_record(
        cluster="ares",
        session_id="desktop-session-1",
        session_generation_id="generation-1",
        remote_api_port=8765,
        path=path,
    )

    save_owned_session_record(
        cluster="ares",
        session_id="desktop-session-2",
        session_generation_id="generation-2",
        remote_api_port=9001,
        path=path,
    )

    loaded = load_owned_session_record("ares", path=path)
    assert loaded is not None
    assert loaded.session_id == "desktop-session-2"
    assert loaded.session_generation_id == "generation-2"
    assert loaded.remote_api_port == 9001


def test_records_for_different_clusters_do_not_clobber_each_other(tmp_path: Path) -> None:
    path = tmp_path / "owned_sessions.json"
    save_owned_session_record(
        cluster="ares",
        session_id="desktop-session-ares",
        session_generation_id="generation-1",
        remote_api_port=8765,
        path=path,
    )
    save_owned_session_record(
        cluster="theta",
        session_id="desktop-session-theta",
        session_generation_id="generation-1",
        remote_api_port=8765,
        path=path,
    )

    ares = load_owned_session_record("ares", path=path)
    theta = load_owned_session_record("theta", path=path)
    assert ares is not None
    assert theta is not None
    assert ares.session_id == "desktop-session-ares"
    assert theta.session_id == "desktop-session-theta"


def test_clear_removes_the_record_for_one_cluster_only(tmp_path: Path) -> None:
    path = tmp_path / "owned_sessions.json"
    save_owned_session_record(
        cluster="ares",
        session_id="desktop-session-ares",
        session_generation_id="generation-1",
        remote_api_port=8765,
        path=path,
    )
    save_owned_session_record(
        cluster="theta",
        session_id="desktop-session-theta",
        session_generation_id="generation-1",
        remote_api_port=8765,
        path=path,
    )

    clear_owned_session_record("ares", path=path)

    assert load_owned_session_record("ares", path=path) is None
    assert load_owned_session_record("theta", path=path) is not None


def test_clear_is_a_no_op_when_no_registry_file_exists_yet(tmp_path: Path) -> None:
    path = tmp_path / "owned_sessions.json"
    clear_owned_session_record("ares", path=path)  # must not raise
    assert load_owned_session_record("ares", path=path) is None


def test_clear_is_a_no_op_when_the_cluster_has_no_record(tmp_path: Path) -> None:
    path = tmp_path / "owned_sessions.json"
    save_owned_session_record(
        cluster="theta",
        session_id="desktop-session-theta",
        session_generation_id="generation-1",
        remote_api_port=8765,
        path=path,
    )

    clear_owned_session_record("ares", path=path)  # never had a record

    assert load_owned_session_record("theta", path=path) is not None


def test_default_path_is_a_sibling_of_the_cluster_registry_by_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(OWNED_SESSION_REGISTRY_ENV, raising=False)
    monkeypatch.delenv("CLIO_RELAY_CLUSTER_REGISTRY", raising=False)

    path = default_owned_session_record_path()

    assert path.name == "owned_sessions.json"
    assert path.parent.name == ".clio-relay"


def test_default_path_honors_its_own_env_override(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    override = tmp_path / "custom" / "sessions.json"
    monkeypatch.setenv(OWNED_SESSION_REGISTRY_ENV, str(override))

    assert default_owned_session_record_path() == override.resolve()
