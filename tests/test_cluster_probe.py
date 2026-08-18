"""Read-only cluster runtime probe that survives a dead pin (clio-relay#158)."""

from __future__ import annotations

from pytest import MonkeyPatch

import clio_relay.cluster_probe as cluster_probe
from clio_relay.cluster_config import ClusterDefinition
from clio_relay.cluster_probe import (
    CLUSTER_PROBE_SCHEMA,
    probe_cluster_runtime,
)
from clio_relay.errors import ObservationTimeoutError

_DEAD_PIN = ClusterDefinition(
    name="ares",
    ssh_host="ares",
    relay_executable="/srv/generations/gone/bin/clio-relay",
    relay_install_receipt="/srv/generations/gone/install-receipt.json",
)


def _canned(output: str) -> object:
    def run(_definition: ClusterDefinition, _script: str) -> str:
        return output

    return run


def test_probe_reports_a_dead_pin_without_running_the_relay(monkeypatch: MonkeyPatch) -> None:
    """Recon against a broken deployment must yield a typed report, not exit 127.

    Every other cluster-targeted command dereferences relay_executable, so a
    dead pin made the deployment unobservable: the operator could not ask what
    was wrong without tripping over the very thing that was wrong.
    """
    monkeypatch.setattr(
        cluster_probe,
        "run_remote_shell",
        _canned(
            "probe_schema=clio-relay.cluster-probe.v1\n"
            "pinned_executable_present=false\n"
            "pinned_receipt_present=false\n"
            "produced_executable_present=true\n"
            "produced_receipt_present=true\n"
        ),
    )

    report = probe_cluster_runtime(_DEAD_PIN)

    assert report["schema_version"] == CLUSTER_PROBE_SCHEMA
    assert report["state"] == "relay_executable_missing"
    assert report["ssh_reachable"] is True
    assert report["relay_executable"] == {
        "path": "/srv/generations/gone/bin/clio-relay",
        "present": False,
    }
    assert report["produced_relay_executable"]["present"] is True
    # The probe must name the repair, not just the symptom.
    assert "cluster bootstrap" in str(report["repair"])


def test_probe_reports_a_healthy_deployment(monkeypatch: MonkeyPatch) -> None:
    monkeypatch.setattr(
        cluster_probe,
        "run_remote_shell",
        _canned(
            "probe_schema=clio-relay.cluster-probe.v1\n"
            "pinned_executable_present=true\n"
            "pinned_receipt_present=true\n"
            "produced_executable_present=true\n"
            "produced_receipt_present=true\n"
        ),
    )

    report = probe_cluster_runtime(_DEAD_PIN)

    assert report["state"] == "ready"
    assert report["repair"] is None


def test_probe_reports_an_unreachable_host_as_typed_state(monkeypatch: MonkeyPatch) -> None:
    """An unreachable host is a distinct state from a broken install."""

    def unreachable(_definition: ClusterDefinition, _script: str) -> str:
        raise ObservationTimeoutError("remote command timed out after 30 seconds: ares")

    monkeypatch.setattr(cluster_probe, "run_remote_shell", unreachable)

    report = probe_cluster_runtime(_DEAD_PIN)

    assert report["ssh_reachable"] is False
    assert report["state"] == "ssh_unreachable"
    assert "timed out" in str(report["detail"])


def test_probe_survives_a_noisy_login_shell(monkeypatch: MonkeyPatch) -> None:
    """Review F4: real hosts wrap command output in MOTD and locale noise.

    The probe runs through ``bash -lc``, so a login banner, a locale warning
    and stray blank lines arrive interleaved with the marker lines. Parsing
    must key on the markers themselves, never on line position.
    """
    monkeypatch.setattr(
        cluster_probe,
        "run_remote_shell",
        _canned(
            "Welcome to Ubuntu 22.04.3 LTS (GNU/Linux 5.15.0 x86_64)\n"
            "\n"
            " * Documentation:  https://help.ubuntu.com\n"
            "bash: warning: setlocale: LC_ALL: cannot change locale (en_US.UTF-8)\n"
            "pinned_executable_present=false\n"
            "Last login: Mon Aug 18 03:14:15 2026 from 10.0.0.5\n"
            "pinned_receipt_present=false\n"
            "   produced_executable_present=true   \n"
            "produced_receipt_present=true\n"
            "\n"
            "some=unrelated=noise\n"
        ),
    )

    report = probe_cluster_runtime(_DEAD_PIN)

    assert report["state"] == "relay_executable_missing"
    assert report["produced_relay_executable"]["present"] is True
    assert report["relay_executable"]["present"] is False


def test_probe_reports_a_relay_that_is_not_installed_at_all(monkeypatch: MonkeyPatch) -> None:
    """Nothing installed is distinct from a pin pointing at the wrong place."""
    monkeypatch.setattr(
        cluster_probe,
        "run_remote_shell",
        _canned(
            "probe_schema=clio-relay.cluster-probe.v1\n"
            "pinned_executable_present=false\n"
            "pinned_receipt_present=false\n"
            "produced_executable_present=false\n"
            "produced_receipt_present=false\n"
        ),
    )

    report = probe_cluster_runtime(_DEAD_PIN)

    assert report["state"] == "relay_not_installed"
    assert "cluster bootstrap" in str(report["repair"])
