"""Tests for the ``gateway`` runtime-lifecycle commands (iowarp/clio-relay#231).

These moved out of ``tests/test_cli.py`` unchanged (beyond import and
patch-target updates) alongside ``resume-runtime``/``attach-runtime``'s
extraction into ``src/clio_relay/cli_gateway_runtime.py``, per ground rule 3
(§2 of ``docs/design/relay-architecture-2026-08.md``). No dedicated
``test_cli.py`` coverage existed for ``start-runtime``/``browser-attach``/
``browser-detach``/``detach-runtime``/``stop-runtime`` beyond
``tests/test_acceptance_report_defaults.py``'s generic acceptance-report
sweep (which patches ``ServiceRuntimeSupervisor`` directly, not anything on
``cli.py``'s namespace, so it needed no changes for this extraction) and the
cross-group-parametrized ``test_cli_cleanup_failure_report_preserves_
requested_policy_from_command_entry`` (stays in ``test_cli.py``).
"""

from __future__ import annotations

import json
from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace

import pytest
from _pytest.monkeypatch import MonkeyPatch
from typer.testing import CliRunner

import clio_relay.cli_remote_worker_attach as cli_remote_worker_attach
import clio_relay.owner_session_admission as owner_session_admission
import clio_relay.service_runtime as service_runtime
import clio_relay.storage_runtime as storage_runtime
from clio_relay import cli
from clio_relay.cli import app
from clio_relay.cluster_config import ClusterDefinition
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.models import GatewaySession, GatewaySessionState
from clio_relay.service_runtime import ServiceRuntimePendingResult
from clio_relay.validation_report import (
    LiveValidationReport,
    ValidationRecorder,
    new_live_validation_report,
)


@pytest.fixture(autouse=True)
def _default_cli_mode(  # pyright: ignore[reportUnusedFunction]
    monkeypatch: MonkeyPatch, tmp_path: Path
) -> None:
    """Mirror ``test_cli.py``'s own autouse fixture's env-var half only.

    Both tests here fake ``cli._require_cluster`` directly, bypassing the
    cluster registry this env var otherwise gates, but every test in
    ``test_cli.py`` ran under it originally -- reproduced here for parity,
    matching ``tests/test_cli_relay_host.py``'s/``tests/test_cli_session.py``'s
    identical precedent.
    """
    monkeypatch.setenv("CLIO_RELAY_CLI_MODE", "local")
    monkeypatch.setenv(
        "CLIO_RELAY_INSTALL_RECEIPT",
        str(tmp_path / "relay-state" / "install-receipt.json"),
    )


def test_gateway_resume_reenters_exact_owner_session_admission(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """Owned pending runtimes cannot resume outside their authoritative generation lock."""
    core_dir = tmp_path / "core"
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(core_dir))
    monkeypatch.setenv("CLIO_RELAY_SPOOL_DIR", str(tmp_path / "spool"))
    queue = ClioCoreQueue(core_dir)
    queue.initialize()
    owner_session_id = "desktop-session"
    owner_generation_id = "generation-1"
    owner_admission_id = cli._desktop_owner_session_admission_id(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        cluster="test-cluster",
        session_id=owner_session_id,
    )
    queue.mirror_owner_session_generation_open(
        owner_admission_id,
        session_generation_id=owner_generation_id,
    )
    gateway = queue.create_gateway_session(
        GatewaySession(
            cluster="test-cluster",
            name="pending-runtime",
            state=GatewaySessionState.PENDING,
            metadata={
                "owner": "clio-relay",
                "owner_session_id": owner_session_id,
                "owner_session_generation_id": owner_generation_id,
                "owner_session_admission_id": owner_admission_id,
            },
        )
    )
    definition = ClusterDefinition(name="test-cluster", ssh_host="test-login")
    events: list[str] = []

    class FakeResult:
        def __init__(self, session: GatewaySession) -> None:
            self.session = session

        def to_live_validation_report(self, **_kwargs: object) -> LiveValidationReport:
            report = new_live_validation_report(
                scenario="gateway-runtime",
                cluster="test-cluster",
            )
            recorder = ValidationRecorder(report)
            with recorder.check("gateway.resume", "resume exact gateway"):
                pass
            recorder.finish()
            return report

    class FakeSupervisor:
        def __init__(self, **_kwargs: object) -> None:
            pass

        def resume_start(self, *, session_id: str) -> FakeResult:
            events.append("resume")
            assert session_id == gateway.session_id
            return FakeResult(queue.get_gateway_session(session_id))

    def admit(**kwargs: object) -> nullcontext[SimpleNamespace]:
        events.append("admit")
        assert kwargs["session_id"] == owner_session_id
        assert kwargs["session_generation_id"] == owner_generation_id
        return nullcontext(
            SimpleNamespace(
                owner_session_id=owner_session_id,
                owner_session_generation_id=owner_generation_id,
                owner_session_admission_id=owner_admission_id,
            )
        )

    def require_cluster(_cluster: str) -> ClusterDefinition:
        return definition

    def selected_queue(_settings: cli.RelaySettings) -> ClioCoreQueue:
        return queue

    def ignore_verified_report(
        _report: LiveValidationReport,
        _definition: ClusterDefinition,
        _path: Path,
    ) -> None:
        return None

    monkeypatch.setattr(cli, "_require_cluster", require_cluster)
    monkeypatch.setattr(storage_runtime, "storage_managed_queue", selected_queue)
    monkeypatch.setattr(service_runtime, "ServiceRuntimeSupervisor", FakeSupervisor)
    monkeypatch.setattr(owner_session_admission, "owner_session_gateway_admission", admit)
    monkeypatch.setattr(
        cli_remote_worker_attach, "_write_remote_verified_report", ignore_verified_report
    )

    result = CliRunner().invoke(
        app,
        [
            "gateway",
            "resume-runtime",
            gateway.session_id,
            "--cluster",
            "test-cluster",
            "--token",
            "token",
            "--secret-key",
            "secret",
        ],
    )

    assert result.exit_code == 0, result.output
    assert events == ["admit", "resume"]


def test_gateway_attach_runtime_prints_pending_resume_contract(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """Attach must not strip a nonterminal gateway's exact retry selector."""
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(tmp_path / "core"))
    monkeypatch.setenv("CLIO_RELAY_SPOOL_DIR", str(tmp_path / "spool"))
    definition = ClusterDefinition(name="test-cluster", ssh_host="test-login")
    gateway = GatewaySession(
        session_id="gateway_pending_attach",
        cluster="test-cluster",
        name="pending-runtime",
        state=GatewaySessionState.STARTING,
        scheduler="slurm",
        scheduler_job_id="12345",
        gateway={
            "connect_url": "http://127.0.0.1:28777",
            "health_url": "http://127.0.0.1:28777/healthz",
            "stream_url": "http://127.0.0.1:28777/live-data",
        },
        metadata={"owner": "clio-relay"},
    )

    class FakeSupervisor:
        def __init__(self, **_kwargs: object) -> None:
            pass

        def attach(self, *, session_id: str) -> ServiceRuntimePendingResult:
            assert session_id == gateway.session_id
            return ServiceRuntimePendingResult(session=gateway)

    def require_cluster(_cluster: str) -> ClusterDefinition:
        return definition

    monkeypatch.setattr(cli, "_require_cluster", require_cluster)
    monkeypatch.setattr(service_runtime, "ServiceRuntimeSupervisor", FakeSupervisor)

    result = CliRunner().invoke(
        app,
        [
            "gateway",
            "attach-runtime",
            gateway.session_id,
            "--cluster",
            definition.name,
            "--token",
            "token",
            "--secret-key",
            "secret",
        ],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["session_id"] == gateway.session_id
    assert payload["outcome"] == "pending"
    assert payload["retry_selector"] == {
        "cluster": definition.name,
        "gateway_session_id": gateway.session_id,
        "scheduler_provider": "slurm",
        "scheduler_job_id": "12345",
    }
    assert payload["scheduler_action"] == "none"
    assert payload["relay_action"] == "none"
    assert payload["scheduler_cancel_requested"] is False
    assert "connect_url" not in payload["gateway"]
    assert "health_url" not in payload["gateway"]
    assert "stream_url" not in payload["gateway"]
