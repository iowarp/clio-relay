"""Tests for the ``relay-host`` command group (iowarp/clio-relay#231, R8(ii)).

These moved out of ``tests/test_cli.py`` unchanged (beyond import and
patch-target updates) alongside the ``relay_host_app`` commands' extraction
into ``src/clio_relay/cli_relay_host.py``, per ground rule 3 (§2 of
``docs/design/relay-architecture-2026-08.md``): a test reachable only
through this command group moves with the logic it exercises. Tests that
exercise the ``relay-host`` group as ONE of several parametrized cases
spanning unrelated command groups (the acceptance-preflight-report and
scheduler-preflight canonical-report tests in ``test_cli.py``) stay there --
they test the shared ``_acceptance_report_command``/
``_write_failed_acceptance_report`` seam, not anything specific to this
group, and moving them would either duplicate them or leave a broken
partial parametrize.

Every ``monkeypatch.setattr`` target here is unchanged from ``test_cli.py``:
``transport_probe.run_*`` patches the owner module directly (already the
R8(i) idiom, unaffected by which file calls it), and
``cli._attach_verified_remote_worker`` still patches ``cli.py`` because that
helper was never part of this extraction -- it stays in ``cli.py``, shared
with session teardown (see ``cli_relay_host.py``'s own docstring for why).

**F2 subprocess regression guard (iowarp/clio-relay#231 R8(ii) review).**
``cli.py`` and ``cli_relay_host.py`` form a real, deliberate import cycle
(see ``cli_relay_host.py``'s own docstring for the discipline that resolves
it). Before that fix, importing ``clio_relay.cli_relay_host`` before
``clio_relay.cli`` in a fresh interpreter raised ``AttributeError:
partially initialized module 'clio_relay.cli_relay_host' has no attribute
'relay_host_app'``, and ``python -m clio_relay.cli`` regressed from exit 0
to exit 1 for the same underlying reason (``runpy`` executes ``cli.py`` as
``__main__``, a distinct module object from ``clio_relay.cli`` in
``sys.modules``, so ``cli_relay_host.py``'s import of ``clio_relay.cli``
mid-way through ``__main__``'s own load re-enters the same cycle). The
three subprocess tests at the bottom of this file are the regression guard:
each spawns a fresh interpreter (in-process re-import cannot reproduce this
-- ``sys.modules`` caching would hide it after the first successful import
in any order) and asserts the exact shape the review measured as broken.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest
from _pytest.monkeypatch import MonkeyPatch
from typer.testing import CliRunner

import clio_relay.cli_remote_worker_attach as cli_remote_worker_attach
import clio_relay.transport_probe as transport_probe
from clio_relay.cli import app
from clio_relay.cluster_config import ClusterDefinition
from clio_relay.errors import ConfigurationError, RelayError
from clio_relay.validation_report import (
    EvidenceReference,
    LiveValidationReport,
    ValidationRecorder,
    ValidationResource,
)
from tests.test_cli import (
    _write_test_cluster,  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)


@pytest.fixture(autouse=True)
def _default_cli_mode(  # pyright: ignore[reportUnusedFunction]
    monkeypatch: MonkeyPatch, tmp_path: Path
) -> None:
    """Mirror ``test_cli.py``'s own autouse fixture's env-var half only.

    That fixture also monkeypatches ``cli._persist_verified_cleanup_report_
    before_closure``/``cli._owned_session_recovery_status`` for session-
    teardown tests; none of the tests in this file exercise that path, so
    only the two environment variables every CLI invocation here relies on
    (local mode, a real install-receipt path under ``tmp_path``) are
    reproduced, to keep this file's only dependency on ``test_cli.py``
    limited to ``_write_test_cluster``.
    """
    monkeypatch.setenv("CLIO_RELAY_CLI_MODE", "local")
    monkeypatch.setenv(
        "CLIO_RELAY_INSTALL_RECEIPT",
        str(tmp_path / "relay-state" / "install-receipt.json"),
    )


def test_cli_tests_ssh_transport(tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    _write_test_cluster(tmp_path)
    calls: list[dict[str, object]] = []

    def fake_probe(**kwargs: object) -> list[str]:
        calls.append(kwargs)
        return [
            "transport.protocol=ssh_forward",
            "transport.healthz=ok",
            "transport.cleanup=passed",
        ]

    monkeypatch.setattr("clio_relay.transport_probe.run_ssh_forward_http_probe", fake_probe)
    monkeypatch.setenv("CLIO_RELAY_API_TOKEN", "api-token")
    report_path = tmp_path / "ssh-transport.json"

    result = CliRunner().invoke(
        app,
        [
            "relay-host",
            "test-ssh-transport",
            "--cluster",
            "ares",
            "--local-bind-port",
            "19001",
            "--remote-api-port",
            "9001",
            "--session-id",
            "session-1",
            "--validation-report",
            str(report_path),
            "--validation-launcher",
            "uvx",
            "--validation-install-source",
            "wheel:clio_relay-1.0.0-py3-none-any.whl",
        ],
    )

    assert result.exit_code == 0
    assert "transport.healthz=ok" in result.output
    assert calls[0]["cluster"] == "ares"
    assert calls[0]["local_bind_port"] == 19001
    assert calls[0]["remote_api_port"] == 9001
    assert calls[0]["session_id"] == "session-1"
    assert calls[0]["api_token"] == "api-token"
    assert calls[0]["detach_remote"] is False
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["status"] == "passed"
    assert report["scenario"] == "transport"
    assert report["install_source"]["launcher"] == "uvx"
    assert {check["check_id"] for check in report["checks"]} >= {
        "transport.ssh",
        "transport.cleanup",
    }
    assert report["resources"] == [
        {
            "cluster": "ares",
            "kind": "connector",
            "metadata": {
                "cleanup_verified": True,
                "remote_session_retained": False,
                "transport_mode": "ssh-forward",
            },
            "provider": None,
            "references": [],
            "resource_id": "session-1",
            "role": "ssh_forward_probe",
            "state": "stopped",
        }
    ]
    assert report["cleanup"]["remaining_resources"] == []


def test_cli_ssh_transport_detach_report_models_retention_without_residual(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    _write_test_cluster(tmp_path)

    def fake_probe(**kwargs: object) -> list[str]:
        assert kwargs["detach_remote"] is True
        return [
            "transport.protocol=ssh_forward",
            "transport.healthz=ok",
            "transport.remote_session=retained",
            "transport.remote_session_ownership=verified",
            "transport.cleanup=detached",
        ]

    monkeypatch.setattr("clio_relay.transport_probe.run_ssh_forward_http_probe", fake_probe)
    monkeypatch.setenv("CLIO_RELAY_API_TOKEN", "api-token")
    report_path = tmp_path / "ssh-detach.json"

    result = CliRunner().invoke(
        app,
        [
            "relay-host",
            "test-ssh-transport",
            "--cluster",
            "ares",
            "--local-bind-port",
            "19011",
            "--session-id",
            "session-detach-1",
            "--detach-remote",
            "--validation-report",
            str(report_path),
        ],
    )

    assert result.exit_code == 0, result.output
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["status"] == "passed"
    assert report["cleanup"]["mode"] == "transport_probe_detach"
    assert report["cleanup"]["remaining_resources"] == []
    resources = {item["kind"]: item for item in report["resources"]}
    assert resources["connector"]["state"] == "stopped"
    assert resources["connector"]["metadata"]["remote_session_retained"] is True
    assert resources["relay_session"]["state"] == "retained"
    assert resources["relay_session"]["metadata"]["verified_after_operation"] is True
    retained_actions = [
        action for action in report["cleanup"]["actions"] if action["action"] == "retain"
    ]
    assert retained_actions[0]["outcome"] == "retained"


def test_cli_tests_http_transport_and_writes_canonical_report(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    _write_test_cluster(tmp_path)
    calls: list[dict[str, object]] = []

    def fake_probe(**kwargs: object) -> list[str]:
        calls.append(kwargs)
        return [
            "transport.protocol=wss",
            "transport.healthz=ok",
            "transport.cleanup=passed",
        ]

    def fake_worker_identity(
        report: LiveValidationReport,
        definition: ClusterDefinition,
        *,
        observed_worker_info: dict[str, object] | None = None,
    ) -> None:
        assert observed_worker_info is None
        assert definition.name == "ares"
        recorder = ValidationRecorder(report)
        with recorder.check("worker.artifact-version", "verified remote worker") as evidence:
            evidence.append(EvidenceReference(kind="test", excerpt="worker verified"))
        recorder.add_resource(
            ValidationResource(
                kind="relay_worker",
                resource_id="worker:ares",
                cluster="ares",
                state="running",
            )
        )

    monkeypatch.setattr("clio_relay.transport_probe.run_frp_http_probe", fake_probe)
    monkeypatch.setattr(
        cli_remote_worker_attach, "_attach_verified_remote_worker", fake_worker_identity
    )
    monkeypatch.setenv("CLIO_RELAY_FRP_TOKEN", "frp-token")
    monkeypatch.setenv("CLIO_RELAY_STCP_SECRET", "stcp-secret")
    report_path = tmp_path / "relay-transport.json"

    result = CliRunner().invoke(
        app,
        [
            "relay-host",
            "test-http-transport",
            "--cluster",
            "ares",
            "--local-bind-port",
            "19002",
            "--remote-api-port",
            "9002",
            "--proxy-name",
            "relay-probe-1",
            "--validation-report",
            str(report_path),
        ],
    )

    assert result.exit_code == 0
    assert calls[0]["token"] == "frp-token"
    assert calls[0]["secret_key"] == "stcp-secret"
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["status"] == "passed"
    assert {check["check_id"] for check in report["checks"]} >= {
        "transport.relay",
        "transport.cleanup",
        "worker.artifact-version",
    }
    assert {resource["kind"] for resource in report["resources"]} == {
        "connector",
        "relay_worker",
    }


def test_cli_tests_direct_transport_and_writes_canonical_report(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    _write_test_cluster(tmp_path)

    def fake_probe(**_kwargs: object) -> list[str]:
        return [
            "direct_transport.result=xtcp",
            "transport.protocol=wss",
            "transport.proxy_type=xtcp",
            "transport.healthz=ok",
            "transport.cleanup=passed",
        ]

    monkeypatch.setattr("clio_relay.transport_probe.run_frp_direct_http_probe", fake_probe)
    monkeypatch.setenv("CLIO_RELAY_FRP_TOKEN", "frp-token")
    monkeypatch.setenv("CLIO_RELAY_STCP_SECRET", "xtcp-secret")
    report_path = tmp_path / "direct-transport.json"

    result = CliRunner().invoke(
        app,
        [
            "relay-host",
            "test-direct-transport",
            "--cluster",
            "ares",
            "--local-bind-port",
            "19004",
            "--proxy-name",
            "direct-probe-1",
            "--no-allow-stcp-fallback",
            "--validation-report",
            str(report_path),
        ],
    )

    assert result.exit_code == 0
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["status"] == "passed"
    assert {check["check_id"] for check in report["checks"]} >= {
        "transport.direct",
        "transport.cleanup",
    }
    assert report["resources"][0]["role"] == "frp_xtcp_probe"


def test_cli_transport_failure_writes_partial_canonical_report(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    _write_test_cluster(tmp_path)

    def failing_probe(**kwargs: object) -> list[str]:
        del kwargs
        raise RelayError("live transport failed")

    monkeypatch.setattr("clio_relay.transport_probe.run_frp_http_probe", failing_probe)
    monkeypatch.setenv("CLIO_RELAY_FRP_TOKEN", "frp-token")
    monkeypatch.setenv("CLIO_RELAY_STCP_SECRET", "stcp-secret")
    report_path = tmp_path / "failed-transport.json"

    result = CliRunner().invoke(
        app,
        [
            "relay-host",
            "test-http-transport",
            "--cluster",
            "ares",
            "--local-bind-port",
            "19003",
            "--proxy-name",
            "relay-probe-failed",
            "--validation-report",
            str(report_path),
        ],
    )

    assert result.exit_code == 1
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["status"] == "failed"
    assert report["checks"][-1]["check_id"] == "transport.completed"
    assert report["checks"][-1]["status"] == "failed"
    assert report["resources"][0]["state"] == "unknown"
    assert report["cleanup"]["remaining_resources"][0]["resource_id"] == ("relay-probe-failed")


def test_cli_transport_worker_identity_failure_fails_canonical_report(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    _write_test_cluster(tmp_path)

    def fake_probe(**_kwargs: object) -> list[str]:
        return [
            "transport.protocol=wss",
            "transport.healthz=ok",
            "transport.cleanup=passed",
        ]

    def fail_worker_identity(
        _report: LiveValidationReport,
        _definition: ClusterDefinition,
        *,
        observed_worker_info: dict[str, object] | None = None,
    ) -> None:
        assert observed_worker_info is None
        raise ConfigurationError("remote wheel hash does not match")

    monkeypatch.setattr(transport_probe, "run_frp_http_probe", fake_probe)
    monkeypatch.setattr(
        cli_remote_worker_attach, "_attach_verified_remote_worker", fail_worker_identity
    )
    monkeypatch.setenv("CLIO_RELAY_FRP_TOKEN", "frp-token")
    monkeypatch.setenv("CLIO_RELAY_STCP_SECRET", "stcp-secret")
    report_path = tmp_path / "worker-mismatch.json"

    result = CliRunner().invoke(
        app,
        [
            "relay-host",
            "test-http-transport",
            "--cluster",
            "ares",
            "--local-bind-port",
            "19005",
            "--validation-report",
            str(report_path),
        ],
    )

    assert result.exit_code == 1
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["status"] == "failed"
    worker_checks = [
        check for check in report["checks"] if check["check_id"] == "worker.installation-info"
    ]
    assert worker_checks[0]["status"] == "failed"


def test_cli_render_frpc_uses_configured_secret_env(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    _write_test_cluster(tmp_path)
    monkeypatch.setenv("CLIO_RELAY_FRP_TOKEN", "env-frp-token")
    monkeypatch.setenv("CLIO_RELAY_STCP_SECRET", "env-stcp-secret")
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "relay-host",
            "render-frpc-config",
            "--cluster",
            "ares",
            "--local-port",
            "8848",
        ],
    )

    assert result.exit_code == 0
    assert 'auth.token = "env-frp-token"' in result.output
    assert 'secretKey = "env-stcp-secret"' in result.output


def test_cli_render_frpc_defaults_proxy_name_to_the_canonical_form(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """clio-relay#279: the `relay-stcp` mismatch trap is closed at the default.

    Before this fix, an operator who rendered this config without an
    explicit `--proxy-name` got the unrelated literal `"relay-stcp"` --
    never matching what the desktop `brokered_tcp`/`udp_rendezvous`
    transports (or a proxy unit installed via `relay-host install-proxy`)
    resolve for the same cluster. This locks the default at the CLI
    boundary, on top of `test_frp_proxy_name_conformance.py`'s own
    module-level proof.
    """
    monkeypatch.chdir(tmp_path)
    _write_test_cluster(tmp_path)
    monkeypatch.setenv("CLIO_RELAY_FRP_TOKEN", "env-frp-token")
    monkeypatch.setenv("CLIO_RELAY_STCP_SECRET", "env-stcp-secret")
    runner = CliRunner()

    result = runner.invoke(
        app,
        ["relay-host", "render-frpc-config", "--cluster", "ares", "--local-port", "8848"],
    )

    assert result.exit_code == 0
    assert 'name = "ares-owned-session"' in result.output
    assert 'name = "relay-stcp"' not in result.output


def test_cli_render_frpc_visitor_defaults_server_name_to_the_canonical_form(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """D1 (adversarial review): the desktop VISITOR side had the identical trap.

    ``render-frpc-visitor-config --server-name`` defaulted to the literal
    ``"relay-stcp"`` while the proxy side (fixed above) now defaults to the
    canonical ``<cluster>-owned-session`` form -- converting a working
    default pairing into a guaranteed-broken one. This proves the SAME
    canonical form comes out of both commands for the same cluster, closing
    the loop `test_frp_proxy_name_conformance.py` proves at the module
    level.
    """
    monkeypatch.chdir(tmp_path)
    _write_test_cluster(tmp_path)
    monkeypatch.setenv("CLIO_RELAY_FRP_TOKEN", "env-frp-token")
    monkeypatch.setenv("CLIO_RELAY_STCP_SECRET", "env-stcp-secret")
    runner = CliRunner()

    proxy_result = runner.invoke(
        app,
        ["relay-host", "render-frpc-config", "--cluster", "ares", "--local-port", "8848"],
    )
    visitor_result = runner.invoke(
        app,
        ["relay-host", "render-frpc-visitor-config", "--cluster", "ares", "--bind-port", "18848"],
    )

    assert proxy_result.exit_code == 0
    assert visitor_result.exit_code == 0
    assert 'name = "ares-owned-session"' in proxy_result.output
    assert 'serverName = "ares-owned-session"' in visitor_result.output
    assert 'serverName = "relay-stcp"' not in visitor_result.output


def test_cli_render_frpc_uses_local_secret_file(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    _write_test_cluster(tmp_path)
    secret_dir = tmp_path / ".clio-relay"
    secret_dir.mkdir(exist_ok=True)
    (secret_dir / "secrets.json").write_text(
        json.dumps(
            {
                "CLIO_RELAY_FRP_TOKEN": "file-frp-token",
                "CLIO_RELAY_STCP_SECRET": "file-stcp-secret",
            }
        ),
        encoding="utf-8-sig",
    )
    monkeypatch.delenv("CLIO_RELAY_FRP_TOKEN", raising=False)
    monkeypatch.delenv("CLIO_RELAY_STCP_SECRET", raising=False)
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "relay-host",
            "render-frpc-config",
            "--cluster",
            "ares",
            "--local-port",
            "8848",
        ],
    )

    assert result.exit_code == 0
    assert 'auth.token = "file-frp-token"' in result.output
    assert 'secretKey = "file-stcp-secret"' in result.output


def test_cli_secret_file_rejects_non_string_secret(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    secret_dir = tmp_path / ".clio-relay"
    secret_dir.mkdir()
    (secret_dir / "secrets.json").write_text(
        json.dumps({"CLIO_RELAY_FRP_TOKEN": 123}),
        encoding="utf-8",
    )
    monkeypatch.delenv("CLIO_RELAY_FRP_TOKEN", raising=False)
    runner = CliRunner()

    result = runner.invoke(
        app,
        ["relay-host", "render-frps-config"],
    )

    assert result.exit_code == 1
    assert "non-empty string" in result.output


def test_cli_transport_reports_missing_configured_secret_env(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    _write_test_cluster(tmp_path)
    monkeypatch.delenv("CLIO_RELAY_FRP_TOKEN", raising=False)
    monkeypatch.delenv("CLIO_RELAY_STCP_SECRET", raising=False)
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "relay-host",
            "render-frpc-config",
            "--cluster",
            "ares",
            "--local-port",
            "8848",
        ],
    )

    assert result.exit_code == 1
    assert "CLIO_RELAY_FRP_TOKEN" in result.output


def test_cli_transport_reports_missing_frp_server_addr(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    _write_test_cluster(tmp_path, frp_server_addr="")
    monkeypatch.setenv("CLIO_RELAY_FRP_TOKEN", "frp-token")
    monkeypatch.setenv("CLIO_RELAY_STCP_SECRET", "secret-key")

    result = CliRunner().invoke(
        app,
        [
            "relay-host",
            "render-frpc-config",
            "--cluster",
            "ares",
            "--local-port",
            "8848",
        ],
    )

    assert result.exit_code == 1
    assert "frp server address is not configured" in result.output


def test_cli_direct_transport_is_strict_xtcp_by_default(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    _write_test_cluster(tmp_path)
    monkeypatch.setenv("CLIO_RELAY_FRP_TOKEN", "frp-token")
    monkeypatch.setenv("CLIO_RELAY_STCP_SECRET", "secret-key")
    calls: list[dict[str, object]] = []

    def fake_direct_probe(**kwargs: object) -> list[str]:
        calls.append(kwargs)
        return ["direct_transport.result=xtcp", "transport.cleanup=passed"]

    monkeypatch.setattr("clio_relay.transport_probe.run_frp_direct_http_probe", fake_direct_probe)

    result = CliRunner().invoke(
        app,
        [
            "relay-host",
            "test-direct-transport",
            "--cluster",
            "ares",
            "--local-bind-port",
            "19000",
        ],
    )

    assert result.exit_code == 0
    assert calls[0]["allow_stcp_fallback"] is False


# ---------------------------------------------------------------------------
# clio-relay#279: cluster-side frpc proxy bring-up commands. Every
# ``frpc_proxy_bringup`` collaborator is patched module-attribute style,
# exactly like ``transport_probe`` above -- these three commands never
# dial anywhere in this test file.
# ---------------------------------------------------------------------------


def test_cli_install_proxy_delegates_to_frpc_proxy_bringup(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    _write_test_cluster(tmp_path)
    calls: list[dict[str, object]] = []

    def fake_install(**kwargs: object) -> object:
        calls.append(kwargs)
        from clio_relay.frpc_proxy_receipt import FrpcProxyBringupReceipt

        return FrpcProxyBringupReceipt(
            cluster="ares",
            proxy_name="ares-owned-session",
            unit_name="clio-relay-frpc-proxy-ares.service",
            toml_path="%h/.config/clio-relay/frpc-proxy-ares.toml",
            env_path="%h/.config/clio-relay/frpc-proxy-ares.env",
            config_sha256="a" * 64,
            enabled=True,
            active=True,
            linger=True,
            persistence="systemd-user-linger",
            installed_at="2026-08-26T00:00:00Z",
        )

    monkeypatch.setattr("clio_relay.frpc_proxy_bringup.install_frpc_proxy_over_ssh", fake_install)

    result = CliRunner().invoke(
        app,
        ["relay-host", "install-proxy", "--cluster", "ares"],
    )

    assert result.exit_code == 0, result.output
    assert calls[0]["cluster"] == "ares"
    assert calls[0]["ssh_host"] == "ares"
    assert '"unit_name": "clio-relay-frpc-proxy-ares.service"' in result.output


def test_cli_install_proxy_respects_an_ssh_host_override(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    _write_test_cluster(tmp_path)
    calls: list[dict[str, object]] = []

    def fake_install(**kwargs: object) -> object:
        calls.append(kwargs)
        raise RelayError("stop before any real dial -- this test only checks argument wiring")

    monkeypatch.setattr("clio_relay.frpc_proxy_bringup.install_frpc_proxy_over_ssh", fake_install)

    result = CliRunner().invoke(
        app,
        ["relay-host", "install-proxy", "--cluster", "ares", "--ssh-host", "ares-jump"],
    )

    assert result.exit_code == 1
    assert calls[0]["ssh_host"] == "ares-jump"


def test_cli_teardown_proxy_delegates_to_frpc_proxy_bringup(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    _write_test_cluster(tmp_path)
    calls: list[dict[str, object]] = []

    def fake_teardown(**kwargs: object) -> object:
        calls.append(kwargs)
        from clio_relay.frpc_proxy_receipt import FrpcProxyTeardownReceipt

        return FrpcProxyTeardownReceipt(
            cluster="ares",
            unit_name="clio-relay-frpc-proxy-ares.service",
            removed_unit=True,
            removed_toml=True,
            removed_env=True,
            torn_down_at="2026-08-26T00:05:00Z",
        )

    monkeypatch.setattr("clio_relay.frpc_proxy_bringup.teardown_frpc_proxy_over_ssh", fake_teardown)

    result = CliRunner().invoke(
        app,
        ["relay-host", "teardown-proxy", "--cluster", "ares"],
    )

    assert result.exit_code == 0, result.output
    assert calls[0]["cluster"] == "ares"
    assert '"removed_unit": true' in result.output


def test_cli_proxy_status_delegates_to_frpc_proxy_bringup(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    _write_test_cluster(tmp_path)
    calls: list[dict[str, object]] = []

    def fake_status(**kwargs: object) -> object:
        calls.append(kwargs)
        from clio_relay.frpc_proxy_receipt import FrpcProxyStatusDocument

        return FrpcProxyStatusDocument(
            cluster="ares",
            unit_name="clio-relay-frpc-proxy-ares.service",
            installed=True,
            enabled=True,
            active=False,
            restart_looping=False,
            load_state="loaded",
            load_state_category="loaded",
            active_state="inactive",
            sub_state="dead",
            journal_tail=["frpc: login to server failed: EOF"],
            diagnosis="frpc proxy unit is inactive (state=inactive/dead)",
        )

    monkeypatch.setattr("clio_relay.frpc_proxy_bringup.frpc_proxy_status_over_ssh", fake_status)

    result = CliRunner().invoke(
        app,
        ["relay-host", "proxy-status", "--cluster", "ares"],
    )

    assert result.exit_code == 0, result.output
    assert calls[0]["cluster"] == "ares"
    assert "frpc: login to server failed: EOF" in result.output


# ---------------------------------------------------------------------------
# D2 (adversarial review, clio-relay#279): all three verbs used to call
# `cli._require_cluster` OUTSIDE `_run_or_exit`'s try/except -- a typo'd
# --cluster raised straight through typer's dispatch as a raw traceback
# instead of `_run_or_exit`'s clean "error: ..." + exit 1. Moved inside
# `action()`, matching the seven pre-existing verbs; these prove it.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "verb_args",
    [
        ["relay-host", "install-proxy", "--cluster", "does-not-exist"],
        ["relay-host", "teardown-proxy", "--cluster", "does-not-exist"],
        ["relay-host", "proxy-status", "--cluster", "does-not-exist"],
    ],
)
def test_cli_frpc_proxy_verbs_report_an_unconfigured_cluster_cleanly(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    verb_args: list[str],
) -> None:
    """D2: a bad ``--cluster`` must be `_run_or_exit`'s clean error, never a raw traceback."""
    monkeypatch.chdir(tmp_path)
    _write_test_cluster(tmp_path)

    def poison(**_kwargs: object) -> object:
        raise AssertionError("must fail resolving --cluster before any ssh-executing call")

    monkeypatch.setattr("clio_relay.frpc_proxy_bringup.install_frpc_proxy_over_ssh", poison)
    monkeypatch.setattr("clio_relay.frpc_proxy_bringup.teardown_frpc_proxy_over_ssh", poison)
    monkeypatch.setattr("clio_relay.frpc_proxy_bringup.frpc_proxy_status_over_ssh", poison)

    result = CliRunner().invoke(app, verb_args)

    assert result.exit_code == 1
    assert "error: cluster is not configured: does-not-exist" in result.output
    assert "Traceback (most recent call last)" not in result.output
    assert result.exception is None or isinstance(result.exception, SystemExit)


# ---------------------------------------------------------------------------
# F2 subprocess regression guard (iowarp/clio-relay#231 R8(ii) review) -- see
# the module docstring. Each test spawns a fresh interpreter: importing
# either module inside THIS test process would reuse whatever import order
# pytest's own collection already established, which can never reproduce a
# reverse-first regression.
# ---------------------------------------------------------------------------


def test_cli_relay_host_reverse_first_import_succeeds() -> None:
    """A fresh interpreter importing ``clio_relay.cli_relay_host`` before
    ``clio_relay.cli`` must succeed. Before the F2 fix this raised
    ``AttributeError: partially initialized module 'clio_relay.cli_relay_host'
    has no attribute 'relay_host_app'`` -- ``cli_relay_host.py`` read
    ``cli.py`` as a module-level runtime import, and ``cli.py``'s own
    module-level import of ``cli_relay_host`` (to register
    ``relay_host_app``) re-entered a still-loading ``cli.py``.
    """
    result = subprocess.run(
        [sys.executable, "-c", "import clio_relay.cli_relay_host"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr


def test_cli_first_import_still_succeeds() -> None:
    """A fresh interpreter importing ``clio_relay.cli`` first must also
    succeed -- the direction that already worked before the F2 fix, guarded
    here so a future change to the cycle can't silently break it while
    fixing (or re-breaking) the reverse direction.
    """
    result = subprocess.run(
        [sys.executable, "-c", "import clio_relay.cli"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr


def test_cli_module_invocation_exits_zero() -> None:
    """``python -m clio_relay.cli`` -- the real end-user launch path -- must
    exit 0. ``runpy`` executes ``cli.py`` as ``__main__``, a module object
    distinct from ``clio_relay.cli`` in ``sys.modules``; ``cli_relay_host.py``
    importing ``clio_relay.cli`` mid-way through ``__main__``'s own load
    then re-enters the same cycle the reverse-first-import test above
    guards, so this regressed the same way: exit 0 -> exit 1 with the same
    ``AttributeError``, before the F2 fix.
    """
    result = subprocess.run(
        [sys.executable, "-m", "clio_relay.cli"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
