"""Unit tests for the durable owned-session record (iowarp/clio-relay#276 B1)."""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from clio_relay.errors import ConfigurationError
from clio_relay.owned_session_record import (
    OWNED_SESSION_RECORD_SCHEMA,
    OWNED_SESSION_REGISTRY_ENV,
    OwnedSessionRecord,
    OwnedSessionRecordRegistry,
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


# --- iowarp/clio-relay#276 review D1: forward compatibility + typed refusals ---


def test_saved_record_carries_its_schema_version(tmp_path: Path) -> None:
    path = tmp_path / "owned_sessions.json"

    saved = save_owned_session_record(
        cluster="ares",
        session_id="desktop-session-1",
        session_generation_id="generation-1",
        remote_api_port=8765,
        path=path,
    )

    assert saved.schema_version == OWNED_SESSION_RECORD_SCHEMA
    loaded = load_owned_session_record("ares", path=path)
    assert loaded is not None
    assert loaded.schema_version == OWNED_SESSION_RECORD_SCHEMA


def test_corrupt_json_surfaces_as_a_typed_refusal_naming_the_file(tmp_path: Path) -> None:
    path = tmp_path / "owned_sessions.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("{not valid json", encoding="utf-8")

    with pytest.raises(ConfigurationError, match=re.escape(str(path))) as excinfo:
        load_owned_session_record("ares", path=path)

    # Actionable, not a raw pydantic/json traceback: names the recovery action.
    assert "session start" in str(excinfo.value)


def test_a_record_missing_a_required_field_surfaces_as_a_typed_refusal(tmp_path: Path) -> None:
    path = tmp_path / "owned_sessions.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "sessions": {
                    "ares": {
                        "schema_version": OWNED_SESSION_RECORD_SCHEMA,
                        "cluster": "ares",
                        "session_id": "desktop-session-1",
                        # session_generation_id, remote_api_port, created_at all omitted.
                    }
                }
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(ConfigurationError, match=re.escape(str(path))):
        load_owned_session_record("ares", path=path)


def test_an_unrecognized_schema_version_surfaces_as_a_typed_refusal(tmp_path: Path) -> None:
    path = tmp_path / "owned_sessions.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "sessions": {
                    "ares": {
                        "schema_version": "clio-relay.owned-session-record.v2",
                        "cluster": "ares",
                        "session_id": "desktop-session-1",
                        "session_generation_id": "generation-1",
                        "remote_api_port": 8765,
                        "created_at": "2026-01-01T00:00:00+00:00",
                    }
                }
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(ConfigurationError, match=re.escape(str(path))):
        load_owned_session_record("ares", path=path)


def test_a_field_a_newer_client_wrote_is_tolerated_and_preserved(tmp_path: Path) -> None:
    """extra="allow": an additive field this build does not know about must
    round-trip through a read-modify-write instead of vanishing or crashing."""
    path = tmp_path / "owned_sessions.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "sessions": {
                    "ares": {
                        "schema_version": OWNED_SESSION_RECORD_SCHEMA,
                        "cluster": "ares",
                        "session_id": "desktop-session-1",
                        "session_generation_id": "generation-1",
                        "remote_api_port": 8765,
                        "created_at": "2026-01-01T00:00:00+00:00",
                        "transport_mode": "brokered_tcp",
                    }
                },
                "future_registry_field": "kept",
            }
        ),
        encoding="utf-8",
    )

    loaded = load_owned_session_record("ares", path=path)
    assert loaded is not None
    assert loaded.model_dump()["transport_mode"] == "brokered_tcp"

    # A read-modify-write for an UNRELATED cluster must not drop either
    # unknown field.
    save_owned_session_record(
        cluster="theta",
        session_id="desktop-session-theta",
        session_generation_id="generation-1",
        remote_api_port=9001,
        path=path,
    )

    registry = OwnedSessionRecordRegistry.load(path)
    assert registry.model_dump()["future_registry_field"] == "kept"
    assert registry.sessions["ares"].model_dump()["transport_mode"] == "brokered_tcp"
    assert registry.sessions["theta"].session_id == "desktop-session-theta"


def test_owned_session_record_model_tolerates_unknown_fields_directly() -> None:
    record = OwnedSessionRecord.model_validate(
        {
            "schema_version": OWNED_SESSION_RECORD_SCHEMA,
            "cluster": "ares",
            "session_id": "desktop-session-1",
            "session_generation_id": "generation-1",
            "remote_api_port": 8765,
            "created_at": "2026-01-01T00:00:00+00:00",
            "some_future_field": 42,
        }
    )
    assert record.model_dump()["some_future_field"] == 42
