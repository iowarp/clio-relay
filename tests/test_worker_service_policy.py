"""Tests for persisted managed-worker capacity and safe service restart."""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path
from typing import cast

import pytest
from pydantic import ValidationError
from typer.testing import CliRunner

from clio_relay import cli, deployment
from clio_relay.cli import app
from clio_relay.cluster_config import (
    ClusterDefinition,
    ClusterRegistry,
    WorkerCapacityPolicy,
)
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.deployment import render_endpoint_user_service
from clio_relay.models import JobKind


def test_managed_worker_capacity_defaults_are_safe_and_durable(tmp_path: Path) -> None:
    """Legacy definitions acquire one reserved control slot on load and save."""
    definition = ClusterDefinition.model_validate({"name": "alpha", "ssh_host": "alpha-login"})

    assert definition.worker_capacity == WorkerCapacityPolicy(
        concurrency=3,
        control_query_concurrency=1,
    )

    registry_path = tmp_path / "clusters.json"
    ClusterRegistry(clusters={"alpha": definition}).save(registry_path)
    loaded = ClusterRegistry.load(registry_path).require("alpha")

    assert loaded.worker_capacity == definition.worker_capacity
    payload = registry_path.read_text(encoding="utf-8")
    assert '"concurrency": 3' in payload
    assert '"control_query_concurrency": 1' in payload


@pytest.mark.parametrize(
    "policy",
    [
        {"concurrency": 1, "control_query_concurrency": 1},
        {"concurrency": 3, "control_query_concurrency": 0},
        {"concurrency": 3, "control_query_concurrency": 3},
        {"concurrency": True, "control_query_concurrency": 1},
        {
            "concurrency": 3,
            "control_query_concurrency": 1,
            "kind_concurrency": {"unknown": 1},
        },
    ],
)
def test_managed_worker_capacity_rejects_unsafe_policy(policy: dict[str, object]) -> None:
    """Managed policies always retain both a workload and a control-query slot."""
    with pytest.raises(ValidationError):
        WorkerCapacityPolicy.model_validate(policy)


def test_user_service_uses_the_persisted_cluster_capacity_policy() -> None:
    """Rendering without overrides cannot fall back to the old one-slot default."""
    definition = ClusterDefinition(
        name="alpha",
        ssh_host="alpha-login",
        scheduler_provider="slurm",
        worker_capacity=WorkerCapacityPolicy(
            concurrency=5,
            control_query_concurrency=2,
            kind_concurrency={JobKind.JARVIS: 3},
        ),
    )

    rendered = render_endpoint_user_service(cluster="alpha", definition=definition)

    assert (
        "--concurrency 5 --control-query-concurrency 2 "
        "--kind-concurrency jarvis=3 --scheduler-provider slurm"
    ) in rendered


def test_cluster_add_persists_generic_worker_capacity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Capacity is operator-configured per cluster rather than keyed to a site name."""
    monkeypatch.chdir(tmp_path)

    result = CliRunner().invoke(
        app,
        [
            "cluster",
            "add",
            "--name",
            "arbitrary-site",
            "--ssh-host",
            "arbitrary-login",
            "--worker-concurrency",
            "8",
            "--worker-control-query-concurrency",
            "2",
            "--worker-kind-concurrency",
            "jarvis=4",
        ],
    )

    assert result.exit_code == 0, result.output
    capacity = (
        ClusterRegistry.load(tmp_path / ".clio-relay" / "clusters.json")
        .require("arbitrary-site")
        .worker_capacity
    )
    assert capacity == WorkerCapacityPolicy(
        concurrency=8,
        control_query_concurrency=2,
        kind_concurrency={JobKind.JARVIS: 4},
    )


def test_endpoint_cli_passes_reserved_control_capacity_to_worker(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The service-level flag reaches durable endpoint capacity metadata."""
    core = tmp_path / "core"
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(core))
    monkeypatch.setenv("CLIO_RELAY_SPOOL_DIR", str(tmp_path / "spool"))

    result = CliRunner().invoke(
        app,
        [
            "endpoint",
            "start",
            "--role",
            "worker",
            "--cluster",
            "alpha",
            "--scheduler-provider",
            "external",
            "--once",
            "--concurrency",
            "3",
            "--control-query-concurrency",
            "1",
        ],
    )

    assert result.exit_code == 0, result.output
    endpoints = ClioCoreQueue(core).list_endpoints(cluster="alpha")
    assert len(endpoints) == 1
    assert endpoints[0].metadata["concurrency"] == 3
    assert endpoints[0].metadata["workload_concurrency"] == 2
    assert endpoints[0].metadata["control_query_concurrency"] == 1


def test_install_service_persists_explicit_capacity_overrides(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A later reinstall reuses the exact operator-selected capacity policy."""
    monkeypatch.chdir(tmp_path)
    registry_path = tmp_path / ".clio-relay" / "clusters.json"
    ClusterRegistry(
        clusters={
            "alpha": ClusterDefinition(
                name="alpha",
                ssh_host="alpha-login",
                worker_capacity=WorkerCapacityPolicy(
                    concurrency=4,
                    control_query_concurrency=1,
                    kind_concurrency={JobKind.JARVIS: 2},
                ),
            )
        }
    ).save(registry_path)
    rendered_units: list[str] = []

    def install(
        *,
        cluster: str,
        ssh_host: str,
        service_text: str,
        start: bool,
        enable: bool,
        require_persistent: bool,
        timeout_seconds: float = 120.0,
    ) -> list[str]:
        del start, enable, require_persistent, timeout_seconds
        assert cluster == "alpha"
        assert ssh_host == "alpha-login"
        rendered_units.append(service_text)
        return ["endpoint_service.active=active"]

    monkeypatch.setattr(cli, "install_endpoint_user_service_over_ssh", install)
    runner = CliRunner()

    changed = runner.invoke(
        app,
        [
            "cluster",
            "install-endpoint-service",
            "--cluster",
            "alpha",
            "--concurrency",
            "6",
            "--control-query-concurrency",
            "2",
            "--kind-concurrency",
            "remote_agent=3",
        ],
    )
    repeated = runner.invoke(
        app,
        ["cluster", "install-endpoint-service", "--cluster", "alpha"],
    )

    assert changed.exit_code == 0, changed.output
    assert repeated.exit_code == 0, repeated.output
    persisted = ClusterRegistry.load(registry_path).require("alpha").worker_capacity
    assert persisted == WorkerCapacityPolicy(
        concurrency=6,
        control_query_concurrency=2,
        kind_concurrency={JobKind.REMOTE_AGENT: 3},
    )
    expected = "--concurrency 6 --control-query-concurrency 2 --kind-concurrency remote_agent=3"
    assert len(rendered_units) == 2
    assert all(expected in unit for unit in rendered_units)


def test_restart_service_cli_preserves_registry_and_never_installs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The restart-only command cannot rewrite a unit through the install helper."""
    monkeypatch.chdir(tmp_path)
    registry_path = tmp_path / ".clio-relay" / "clusters.json"
    ClusterRegistry(
        clusters={
            "alpha": ClusterDefinition(
                name="alpha",
                ssh_host="alpha-login",
                worker_capacity=WorkerCapacityPolicy(
                    concurrency=7,
                    control_query_concurrency=2,
                ),
            )
        }
    ).save(registry_path)
    before = registry_path.read_bytes()
    calls: list[tuple[str, str, WorkerCapacityPolicy, bool]] = []

    def restart(
        *,
        cluster: str,
        ssh_host: str,
        expected_capacity: WorkerCapacityPolicy,
        require_persistent: bool,
        timeout_seconds: float = 120.0,
    ) -> list[str]:
        del timeout_seconds
        calls.append((cluster, ssh_host, expected_capacity, require_persistent))
        return [
            "endpoint_service.unit_rewritten=false",
            "endpoint_service.policy_source=installed-unit",
            "endpoint_service.policy_validated=true",
        ]

    def fail_install(**_kwargs: object) -> list[str]:
        raise AssertionError("restart-only must not call the installer")

    monkeypatch.setattr(cli, "restart_endpoint_user_service_over_ssh", restart)
    monkeypatch.setattr(cli, "install_endpoint_user_service_over_ssh", fail_install)

    result = CliRunner().invoke(
        app,
        ["cluster", "restart-endpoint-service", "--cluster", "alpha"],
    )

    assert result.exit_code == 0, result.output
    assert calls == [
        (
            "alpha",
            "alpha-login",
            WorkerCapacityPolicy(concurrency=7, control_query_concurrency=2),
            True,
        )
    ]
    assert registry_path.read_bytes() == before
    assert "endpoint_service.unit_rewritten=false" in result.output
    assert "endpoint_service.policy_validated=true" in result.output


def test_remote_restart_script_controls_existing_unit_without_rewriting_it() -> None:
    """Restart-only shell behavior reads service state and never emits unit content."""
    bash = shutil.which("bash")
    if bash is None:
        pytest.fail("bash is required to validate the remote endpoint restart")
    expected_capacity = WorkerCapacityPolicy(
        concurrency=7,
        control_query_concurrency=2,
        kind_concurrency={JobKind.JARVIS: 3, JobKind.REMOTE_AGENT: 2},
    )
    script = deployment._remote_restart_script(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        service_name="clio-relay-worker-test.service",
        expected_capacity=expected_capacity,
        require_persistent=True,
    )
    assert '> "$HOME/.config/systemd/user' not in script
    assert "daemon-reload" not in script
    assert "systemctl --user enable" not in script
    harness = f"""set -u
export USER=test-user
loginctl() {{ echo yes; }}
systemctl() {{
  echo "systemctl=$*" >&2
  case "${{2:-}}" in
    is-enabled) echo enabled ;;
    show)
      echo "argv[]=/home/test/.local/bin/clio-relay endpoint start --role worker" \
        "--cluster test --concurrency 7 --control-query-concurrency 2" \
        "--kind-concurrency jarvis=3 --kind-concurrency remote_agent=2" \
        "--scheduler-provider external ;"
      ;;
    is-active) echo active ;;
    is-system-running) echo running ;;
  esac
}}
{script}
"""

    result = subprocess.run(
        [bash, "-s"],
        input=harness.encode("utf-8"),
        capture_output=True,
        check=False,
        timeout=30,
    )
    stdout = result.stdout.decode("utf-8", errors="replace")
    stderr = result.stderr.decode("utf-8", errors="replace")

    assert result.returncode == 0, stderr
    assert "endpoint_service.unit_rewritten=false" in stdout
    assert "endpoint_service.policy_source=installed-unit" in stdout
    assert "endpoint_service.policy_validated=true" in stdout
    assert "systemctl=--user restart clio-relay-worker-test.service" in stderr
    assert "daemon-reload" not in stderr
    assert "systemctl=--user enable " not in stderr


def test_remote_restart_script_refuses_policy_mismatch_before_restart() -> None:
    """A legacy or drifted unit cannot be restarted as though it were policy-safe."""
    bash = shutil.which("bash")
    if bash is None:
        pytest.fail("bash is required to validate the remote endpoint restart")
    script = deployment._remote_restart_script(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        service_name="clio-relay-worker-test.service",
        expected_capacity=WorkerCapacityPolicy(
            concurrency=3,
            control_query_concurrency=1,
            kind_concurrency={JobKind.JARVIS: 2},
        ),
        require_persistent=True,
    )
    harness = f"""set -u
export USER=test-user
loginctl() {{ echo yes; }}
systemctl() {{
  echo "systemctl=$*" >&2
  case "${{2:-}}" in
    is-enabled) echo enabled ;;
    show)
      echo "argv[]=/home/test/.local/bin/clio-relay endpoint start --role worker" \
        "--cluster test --concurrency 1 --scheduler-provider external ;"
      ;;
    is-active) echo active ;;
    is-system-running) echo running ;;
  esac
}}
{script}
"""

    result = subprocess.run(
        [bash, "-s"],
        input=harness.encode("utf-8"),
        capture_output=True,
        check=False,
        timeout=30,
    )
    stdout = result.stdout.decode("utf-8", errors="replace")
    stderr = result.stderr.decode("utf-8", errors="replace")

    assert result.returncode == 79
    assert "capacity policy does not match the persisted cluster policy" in stderr
    assert "expected concurrency=3 control_query_concurrency=1 kind_concurrency=jarvis=2" in stderr
    assert "observed concurrency=1 control_query_concurrency=missing" in stderr
    assert "cluster install-endpoint-service" in stderr
    assert "systemctl=--user restart" not in stderr
    assert "endpoint_service.unit_rewritten" not in stdout


def test_restart_helper_uses_bounded_ssh_without_service_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The public restart helper sends only the generated control script."""
    observed: dict[str, object] = {}

    def run(
        command: list[str],
        *,
        input: bytes,
        capture_output: bool,
        check: bool,
        timeout: float,
    ) -> subprocess.CompletedProcess[bytes]:
        assert capture_output is True
        assert check is False
        observed.update(command=command, input=input, timeout=timeout)
        return subprocess.CompletedProcess(
            command,
            0,
            b"endpoint_service.unit_rewritten=false\n",
            b"",
        )

    monkeypatch.setattr(deployment.subprocess, "run", run)

    lines = deployment.restart_endpoint_user_service_over_ssh(
        cluster="alpha",
        ssh_host="alpha-login",
        expected_capacity=WorkerCapacityPolicy(
            concurrency=5,
            control_query_concurrency=2,
            kind_concurrency={JobKind.REMOTE_AGENT: 3},
        ),
        timeout_seconds=17,
    )

    assert lines == ["endpoint_service.unit_rewritten=false"]
    assert observed["command"] == ["ssh", "alpha-login", "bash", "-s"]
    assert observed["timeout"] == 17
    sent = cast(bytes, observed["input"]).decode("utf-8")
    assert '> "$HOME/.config/systemd/user' not in sent
    assert "ExecStart=" not in sent
    assert "expected_concurrency=5" in sent
    assert "expected_control_query_concurrency=2" in sent
    assert "expected_kind_concurrency=remote_agent=3" in sent
