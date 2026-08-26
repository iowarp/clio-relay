"""Per-module ssh-budget conformance for iowarp/clio-relay#179's dial-site
burn-down.

For each of the four burned modules (``cli_owned_relay_jobs.py`` +
``cli_owned_relay_jobs_remote_listing.py``, ``cli_owned_scheduler_cancel.
py``, ``cli_remote_worker_probe.py``, ``cli_remote_mcp.py``), two cases:

* A live held channel is registered for the exact identity the operation
  needs: the operation must make ZERO ``remote_cli.run_remote_clio``/``run_
  remote_shell`` calls (monkeypatch-count at the ``remote_cli`` seam) and
  return the correct result, built from the channel's response.
* No live channel is registered (a cold CLI invocation): the operation
  falls back to the pre-existing ssh path (verified via the same fake
  ``remote_cli.run_remote_clio`` seam every existing test in this suite
  already patches), AND the typed ``per_op_ssh_fallback`` reason is
  recorded in :func:`remote_channel_dispatch.per_operation_fallback_ledger`.

Reuses ``tests/test_owned_session_channel.py``'s harness (``_Harness``,
``_install``, ``_connect``, ``_definition``, ``_settings``) -- the same
fake ``http.client.HTTPConnection``/channel-process seam that harness
already proves a real :class:`~clio_relay.remote_connection.RemoteConnection`
against, so "zero new dials, requests observed over the held channel" is
asserted the same way the deployment-gate acceptance test is.
"""

# pyright: reportUnusedFunction=false

from __future__ import annotations

import json
from pathlib import Path
from typing import cast

import pytest

import clio_relay.cli_owned_relay_jobs_remote_listing as cli_owned_relay_jobs_remote_listing
import clio_relay.cli_owned_scheduler_cancel as cli_owned_scheduler_cancel
import clio_relay.cli_remote_mcp as cli_remote_mcp
import clio_relay.cli_remote_worker_probe as cli_remote_worker_probe
import clio_relay.remote_channel_dispatch as remote_channel_dispatch
import clio_relay.remote_cli as remote_cli
from clio_relay.cluster_config import ClusterDefinition
from clio_relay.cluster_config_models import ClusterTargetIdentity
from tests.test_owned_session_channel import (
    _connect,  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    _definition,  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    _Harness,  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    _install,  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)


@pytest.fixture(autouse=True)
def _clear_fallback_ledger() -> None:
    """Every case starts with an empty per-operation ssh-fallback ledger."""
    remote_channel_dispatch.per_operation_fallback_ledger().clear()


def _forbid_remote_cli(monkeypatch: pytest.MonkeyPatch) -> None:
    """Fail loudly if a channel-routed operation still dials ssh per-call."""

    def forbidden(*_args: object, **_kwargs: object) -> str:
        raise AssertionError("a live channel must serve this operation with zero ssh dials")

    monkeypatch.setattr(remote_cli, "run_remote_clio", forbidden)
    monkeypatch.setattr(remote_cli, "run_remote_shell", forbidden)


def _install_for_channel_dispatch(monkeypatch: pytest.MonkeyPatch, harness: _Harness) -> _Harness:
    """``_install`` plus pointing ``remote_channel_dispatch`` at the fake registry."""
    _install(monkeypatch, harness)
    monkeypatch.setattr(remote_channel_dispatch, "connection_registry", lambda: harness.registry)
    return harness


def _cold_registry(monkeypatch: pytest.MonkeyPatch) -> None:
    """No live channel is held for any cluster -- every dispatch cold-falls-back."""
    from clio_relay.remote_connection import RemoteConnectionRegistry

    monkeypatch.setattr(
        remote_channel_dispatch, "connection_registry", lambda: RemoteConnectionRegistry()
    )


def _fallback_reasons(operation: str) -> list[str]:
    return [
        entry.detail
        for entry in remote_channel_dispatch.per_operation_fallback_ledger().report()
        if entry.operation == operation and entry.reason == "per_op_ssh_fallback"
    ]


# ---------------------------------------------------------------------------
# cli_owned_relay_jobs.py / cli_owned_relay_jobs_remote_listing.py
# ---------------------------------------------------------------------------


def test_owned_relay_jobs_listing_rides_live_channel_with_zero_ssh_dials(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("CLIO_RELAY_API_TOKEN", "session-api-token")
    harness = _install_for_channel_dispatch(monkeypatch, _Harness())
    _connect(tmp_path, harness)
    harness.responses["/queue"] = {
        "jobs": [],
        "source_next_cursor": None,
        "visibility_filter": "exact_owner_session_generation",
    }
    _forbid_remote_cli(monkeypatch)

    jobs = cli_owned_relay_jobs_remote_listing.list_remote_owned_active_cluster_jobs(
        _definition(),
        "ares",
        owner_session_id="desktop-session-1",
        owner_session_generation_id="generation-1",
    )

    assert jobs == []
    assert harness.dials == 1
    assert any(cast(str, request["path"]).startswith("/queue") for request in harness.requests)


def test_owned_relay_jobs_listing_falls_back_to_ssh_when_cold(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _cold_registry(monkeypatch)
    calls: list[list[str]] = []

    def fake_remote(_definition: ClusterDefinition, args: list[str]) -> str:
        calls.append(args)
        return json.dumps(
            {
                "jobs": [],
                "source_cursor": 1,
                "source_limit": 500,
                "source_next_cursor": None,
                "source_total": 0,
                "source_window_count": 0,
            }
        )

    monkeypatch.setattr(remote_cli, "run_remote_clio", fake_remote)

    jobs = cli_owned_relay_jobs_remote_listing.list_remote_owned_active_cluster_jobs(
        _definition(),
        "ares",
        owner_session_id="desktop-session-1",
        owner_session_generation_id="generation-1",
    )

    assert jobs == []
    assert len(calls) == 1
    assert calls[0][:2] == ["queue", "owner-jobs"]
    assert _fallback_reasons("list_remote_owned_active_cluster_jobs")


# ---------------------------------------------------------------------------
# cli_owned_scheduler_cancel.py
# ---------------------------------------------------------------------------


def test_scheduler_status_rides_live_channel_with_zero_ssh_dials(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("CLIO_RELAY_API_TOKEN", "session-api-token")
    harness = _install_for_channel_dispatch(monkeypatch, _Harness())
    _connect(tmp_path, harness)
    harness.responses["/scheduler/jobs/12345/status"] = {
        "scheduler": "slurm",
        "scheduler_job_id": "12345",
        "phase": "running",
    }
    _forbid_remote_cli(monkeypatch)

    phase, error = cli_owned_scheduler_cancel._scheduler_phase_after_operation(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        _definition(),
        "12345",
        provider="slurm",
        owner_session_id="desktop-session-1",
        owner_session_generation_id="generation-1",
    )

    assert (phase, error) == ("running", None)
    assert harness.dials == 1
    assert any(
        cast(str, request["path"]).startswith("/scheduler/jobs/12345/status")
        for request in harness.requests
    )


def test_scheduler_status_falls_back_to_ssh_when_cold(monkeypatch: pytest.MonkeyPatch) -> None:
    _cold_registry(monkeypatch)
    calls: list[list[str]] = []

    def fake_remote(_definition: ClusterDefinition, args: list[str]) -> str:
        calls.append(args)
        return json.dumps({"scheduler": "slurm", "scheduler_job_id": "12345", "phase": "running"})

    monkeypatch.setattr(remote_cli, "run_remote_clio", fake_remote)
    monkeypatch.setenv("CLIO_RELAY_CLI_MODE", "ssh")

    phase, error = cli_owned_scheduler_cancel._scheduler_phase_after_operation(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        _definition(),
        "12345",
        provider="slurm",
        owner_session_id="desktop-session-1",
        owner_session_generation_id="generation-1",
    )

    assert (phase, error) == ("running", None)
    assert len(calls) == 1
    assert calls[0][:2] == ["scheduler", "status"]
    assert _fallback_reasons("scheduler_status")


# ---------------------------------------------------------------------------
# cli_remote_worker_probe.py (ambient identity: no caller-explicit session id)
# ---------------------------------------------------------------------------


def _target_identity_definition() -> ClusterDefinition:
    return ClusterDefinition(
        name="ares",
        ssh_host="ares-login",
        scheduler_provider="external",
        target_identity=ClusterTargetIdentity(
            hostnames=["ares.example.org"],
            ssh_host_key_sha256=["SHA256:fakefingerprint"],
        ),
    )


def _set_ambient_identity(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CLIO_RELAY_API_TOKEN", "session-api-token")
    monkeypatch.setenv("CLIO_RELAY_OWNER_SESSION_ID", "desktop-session-1")
    monkeypatch.setenv("CLIO_RELAY_SESSION_GENERATION_ID", "generation-1")
    monkeypatch.setenv("CLIO_RELAY_OWNER_SESSION_CLUSTER", "ares")


def _fake_ssh_host_key_fingerprints(_ssh_host: str, *, deadline: float | None = None) -> list[str]:
    del deadline
    return ["SHA256:fakefingerprint"]


def test_remote_target_identity_rides_live_channel_with_zero_ssh_dials(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    harness = _install_for_channel_dispatch(monkeypatch, _Harness())
    _connect(tmp_path, harness)
    _set_ambient_identity(monkeypatch)
    harness.responses["/target-info"] = {
        "schema_version": "clio-relay.cluster-target-info.v1",
        "hostname": "ares.example.org",
        "fqdn": "ares.example.org",
        "site_marker_sha256": None,
        "scheduler_provider": "external",
        "scheduler_cluster_name": None,
    }
    monkeypatch.setattr(
        cli_remote_worker_probe,
        "_ssh_host_key_fingerprints",
        _fake_ssh_host_key_fingerprints,
    )
    _forbid_remote_cli(monkeypatch)

    result = cli_remote_worker_probe._remote_target_identity(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        _target_identity_definition()
    )

    assert result["verified"] is True
    assert harness.dials == 1
    assert any(
        cast(str, request["path"]).startswith("/target-info") for request in harness.requests
    )


def test_remote_target_identity_falls_back_to_ssh_when_cold(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _cold_registry(monkeypatch)
    _set_ambient_identity(monkeypatch)
    calls: list[list[str]] = []

    def fake_remote(_definition: ClusterDefinition, args: list[str]) -> str:
        calls.append(args)
        return json.dumps(
            {
                "schema_version": "clio-relay.cluster-target-info.v1",
                "hostname": "ares.example.org",
                "fqdn": "ares.example.org",
                "site_marker_sha256": None,
                "scheduler_provider": "external",
                "scheduler_cluster_name": None,
            }
        )

    monkeypatch.setattr(remote_cli, "run_remote_clio", fake_remote)
    monkeypatch.setattr(
        cli_remote_worker_probe,
        "_ssh_host_key_fingerprints",
        _fake_ssh_host_key_fingerprints,
    )

    result = cli_remote_worker_probe._remote_target_identity(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        _target_identity_definition()
    )

    assert result["verified"] is True
    assert len(calls) == 1
    assert calls[0][:2] == ["endpoint", "target-info"]
    assert _fallback_reasons("remote_target_identity")


# ---------------------------------------------------------------------------
# cli_remote_mcp.py (ambient identity: no caller-explicit session id)
# ---------------------------------------------------------------------------


def _fake_remote_artifact_records(
    _definition: ClusterDefinition, _job_id: str
) -> list[dict[str, object]]:
    return [{"artifact_id": "artifact-1", "kind": "mcp_result"}]


def _fake_decode_artifact_envelope(envelope: object) -> bytes:
    return json.dumps(envelope).encode("utf-8")


def test_mcp_result_artifact_read_rides_live_channel_with_zero_ssh_dials(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    harness = _install_for_channel_dispatch(monkeypatch, _Harness())
    _connect(tmp_path, harness)
    _set_ambient_identity(monkeypatch)
    harness.responses["/artifacts/artifact-1/content"] = {
        "encoding": "base64",
        "content": "eyJvayI6IHRydWV9",
    }
    monkeypatch.setattr(
        "clio_relay.cli_jarvis_artifact_io._remote_artifact_records",
        _fake_remote_artifact_records,
    )
    monkeypatch.setattr(
        "clio_relay.cli_jarvis_artifact_io._decode_artifact_envelope",
        _fake_decode_artifact_envelope,
    )
    _forbid_remote_cli(monkeypatch)

    artifact, payload = cli_remote_mcp._read_remote_mcp_result_artifact(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        _definition(), "job-1"
    )

    assert artifact["artifact_id"] == "artifact-1"
    assert b"content" in payload
    assert harness.dials == 1
    assert any(
        cast(str, request["path"]).startswith("/artifacts/artifact-1/content")
        for request in harness.requests
    )


def test_mcp_result_artifact_read_falls_back_to_ssh_when_cold(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _cold_registry(monkeypatch)
    _set_ambient_identity(monkeypatch)
    calls: list[list[str]] = []

    def fake_remote(_definition: ClusterDefinition, args: list[str]) -> str:
        calls.append(args)
        return json.dumps({"encoding": "base64", "content": "eyJvayI6IHRydWV9"})

    monkeypatch.setattr(remote_cli, "run_remote_clio", fake_remote)
    monkeypatch.setattr(
        "clio_relay.cli_jarvis_artifact_io._remote_artifact_records",
        _fake_remote_artifact_records,
    )
    monkeypatch.setattr(
        "clio_relay.cli_jarvis_artifact_io._decode_artifact_envelope",
        _fake_decode_artifact_envelope,
    )

    artifact, payload = cli_remote_mcp._read_remote_mcp_result_artifact(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        _definition(), "job-1"
    )

    assert artifact["artifact_id"] == "artifact-1"
    assert b"content" in payload
    assert len(calls) == 1
    assert calls[0][:2] == ["job", "read-artifact"]
    assert _fallback_reasons("read_remote_mcp_result_artifact")
