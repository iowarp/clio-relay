from __future__ import annotations

from pathlib import Path

import pytest
from pytest import MonkeyPatch

from clio_relay.config import RelaySettings


def test_relay_settings_load_log_capture_quotas_from_environment(
    monkeypatch: MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(tmp_path / "core"))
    monkeypatch.setenv("CLIO_RELAY_SPOOL_DIR", str(tmp_path / "spool"))
    monkeypatch.setenv("CLIO_RELAY_SPOOL_MAX_LOG_BYTES_PER_STREAM", "1234")
    monkeypatch.setenv("CLIO_RELAY_SPOOL_MAX_LOG_BYTES_PER_JOB", "2345")

    settings = RelaySettings.from_env()

    assert settings.spool_max_log_bytes_per_stream == 1234
    assert settings.spool_max_log_bytes_per_job == 2345


def test_relay_settings_load_complete_storage_policy_from_environment(
    monkeypatch: MonkeyPatch,
) -> None:
    values = {
        "CLIO_RELAY_SPOOL_MAX_LOG_BYTES_PER_JOB": "100",
        "CLIO_RELAY_STORAGE_CORE_HIGH_WATER_BYTES": "10000",
        "CLIO_RELAY_STORAGE_SPOOL_HIGH_WATER_BYTES": "20000",
        "CLIO_RELAY_STORAGE_TOTAL_HIGH_WATER_BYTES": "30000",
        "CLIO_RELAY_STORAGE_MINIMUM_FREE_BYTES": "0",
        "CLIO_RELAY_STORAGE_MAX_JOB_RESERVATION_BYTES": "1000",
        "CLIO_RELAY_STORAGE_MAX_SCAN_ENTRIES": "101",
        "CLIO_RELAY_STORAGE_MAX_SCAN_DEPTH": "12",
        "CLIO_RELAY_STORAGE_MAX_SCAN_ACCOUNTED_BYTES": "40000",
        "CLIO_RELAY_STORAGE_MAX_LEDGER_BYTES": "50000",
        "CLIO_RELAY_STORAGE_MAX_RESERVATIONS": "99",
        "CLIO_RELAY_STORAGE_LOCK_TIMEOUT_SECONDS": "1.5",
        "CLIO_RELAY_STORAGE_JOB_CORE_ALLOWANCE_BYTES": "20",
        "CLIO_RELAY_STORAGE_JOB_RESULT_ALLOWANCE_BYTES": "30",
        "CLIO_RELAY_STORAGE_RUNTIME_CHECK_INTERVAL_SECONDS": "0.25",
    }
    for name, value in values.items():
        monkeypatch.setenv(name, value)

    settings = RelaySettings.from_env()
    limits = settings.storage_limits()

    assert limits.core_high_water_bytes == 10_000
    assert limits.spool_high_water_bytes == 20_000
    assert limits.total_high_water_bytes == 30_000
    assert limits.minimum_free_bytes == 0
    assert limits.max_job_reservation_bytes == 1_000
    assert limits.max_scan_entries == 101
    assert limits.max_scan_depth == 12
    assert limits.max_scan_accounted_bytes == 40_000
    assert limits.max_ledger_bytes == 50_000
    assert limits.max_reservations == 99
    assert limits.lock_timeout_seconds == 1.5
    assert settings.storage_job_core_allowance_bytes == 20
    assert settings.storage_job_result_allowance_bytes == 30
    assert settings.storage_runtime_check_interval_seconds == 0.25


def test_relay_settings_load_owner_session_generation_from_environment(
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.setenv("CLIO_RELAY_OWNER_SESSION_ID", "desktop-session")
    monkeypatch.setenv("CLIO_RELAY_SESSION_GENERATION_ID", "generation-1")
    monkeypatch.setenv("CLIO_RELAY_OWNER_SESSION_CLUSTER", "ares")

    settings = RelaySettings.from_env()

    assert settings.owner_session_id == "desktop-session"
    assert settings.owner_session_generation_id == "generation-1"
    assert settings.owner_session_cluster == "ares"
    assert settings.resolved_owner_session_cluster() == "ares"
    assert settings.remote_cluster is None


def test_relay_settings_reject_mismatched_owner_and_process_clusters() -> None:
    with pytest.raises(ValueError, match="must match"):
        RelaySettings(
            owner_session_id="desktop-session",
            owner_session_generation_id="generation-1",
            owner_session_cluster="ares",
            remote_cluster="homelab",
        )


@pytest.mark.parametrize(
    ("owner_session_id", "owner_session_generation_id"),
    [("desktop-session", None), (None, "generation-1")],
)
def test_relay_settings_reject_partial_owner_session_identity(
    owner_session_id: str | None,
    owner_session_generation_id: str | None,
) -> None:
    with pytest.raises(ValueError, match="must be configured together"):
        RelaySettings(
            owner_session_id=owner_session_id,
            owner_session_generation_id=owner_session_generation_id,
        )


@pytest.mark.parametrize("value", ["0", "-1", "not-an-integer"])
def test_relay_settings_reject_invalid_log_capture_quota(
    monkeypatch: MonkeyPatch,
    value: str,
) -> None:
    monkeypatch.setenv("CLIO_RELAY_SPOOL_MAX_LOG_BYTES_PER_STREAM", value)

    with pytest.raises(ValueError, match="must be a positive integer"):
        RelaySettings.from_env()


@pytest.mark.parametrize("value", ["-1", "not-an-integer"])
def test_relay_settings_reject_invalid_minimum_free_space(
    monkeypatch: MonkeyPatch,
    value: str,
) -> None:
    monkeypatch.setenv("CLIO_RELAY_STORAGE_MINIMUM_FREE_BYTES", value)

    with pytest.raises(ValueError, match="must be a non-negative integer"):
        RelaySettings.from_env()


@pytest.mark.parametrize("value", ["0", "-1", "nan", "301", "not-a-number"])
def test_relay_settings_reject_invalid_storage_interval(
    monkeypatch: MonkeyPatch,
    value: str,
) -> None:
    monkeypatch.setenv("CLIO_RELAY_STORAGE_RUNTIME_CHECK_INTERVAL_SECONDS", value)

    with pytest.raises(ValueError, match="must be"):
        RelaySettings.from_env()


def test_relay_settings_reject_inconsistent_storage_high_water() -> None:
    with pytest.raises(ValueError, match="at least each individual"):
        RelaySettings(
            storage_core_high_water_bytes=100,
            storage_spool_high_water_bytes=200,
            storage_total_high_water_bytes=199,
        )


def test_relay_settings_reject_default_reservation_above_per_job_cap() -> None:
    with pytest.raises(ValueError, match="default storage reservation exceeds"):
        RelaySettings(
            spool_max_log_bytes_per_job=100,
            storage_job_core_allowance_bytes=20,
            storage_job_result_allowance_bytes=30,
            storage_max_job_reservation_bytes=249,
        )


def test_relay_settings_reject_scan_bound_below_legal_job_reservation() -> None:
    with pytest.raises(ValueError, match="max_scan_accounted_bytes must be at least"):
        RelaySettings(
            storage_max_job_reservation_bytes=1_000,
            storage_max_scan_accounted_bytes=999,
        )


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        (
            {"storage_core_high_water_bytes": 119},
            "default core reservation exceeds",
        ),
        (
            {"storage_spool_high_water_bytes": 129},
            "default spool reservation exceeds",
        ),
        (
            {
                "storage_core_high_water_bytes": 120,
                "storage_spool_high_water_bytes": 130,
                "storage_total_high_water_bytes": 249,
            },
            "default storage reservation exceeds storage total",
        ),
    ],
)
def test_relay_settings_require_default_floors_to_fit_high_water(
    overrides: dict[str, int],
    message: str,
) -> None:
    values: dict[str, object] = {
        "spool_max_log_bytes_per_job": 100,
        "storage_job_core_allowance_bytes": 20,
        "storage_job_result_allowance_bytes": 30,
        "storage_core_high_water_bytes": 1_000,
        "storage_spool_high_water_bytes": 1_000,
        "storage_total_high_water_bytes": 2_000,
        "storage_max_job_reservation_bytes": 1_000,
    }
    values.update(overrides)

    with pytest.raises(ValueError, match=message):
        RelaySettings(**values)  # type: ignore[arg-type]
