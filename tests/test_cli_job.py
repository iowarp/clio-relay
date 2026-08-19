"""Tests for the ``job`` lifecycle command group (iowarp/clio-relay#231).

These moved out of ``tests/test_cli.py`` alongside the four ``job_app``
lifecycle commands' (submit/submit-pipeline/wait/cancel) extraction into
``src/clio_relay/cli_job.py``, per ground rule 3 (SS2 of
``docs/design/relay-architecture-2026-08.md``): a test reachable only
through this command group moves with the logic it exercises. The
thirteen durable-record commands' tests moved separately, to
``tests/test_cli_job_records.py`` (``cli_job_records.py``'s own docstring
explains that second split).

**Known-trap fix.** Two tests called ``cli.job_wait_result(...)`` directly
to compute an expected value -- valid only while ``cli.py`` bare-imported
``job_wait_result`` from ``relay_ops`` into its own namespace (removed in
this same slice, since ``job_wait`` -- the sole remaining caller -- moved
to ``cli_job.py`` and now reaches it via a normal top-level import, not
through ``cli.py``). Both now call ``relay_ops.job_wait_result(...)``
directly, the real owner module, already imported by ``test_cli.py`` and
reproduced here.
``monkeypatch.setattr(relay_ops, "observe_until_terminal", observe)`` and
the ``subprocess.run`` patches already targeted the real owner modules
directly (the R8(i) idiom, unaffected by which file calls it).

**Autouse-fixture parity.** ``test_cli.py`` defines its own module-scoped
``autouse=True`` ``_default_cli_mode`` fixture (env-var defaults every CLI
invocation there relies on). It is reproduced here (the env-var half only,
the same precedent ``tests/test_cli_relay_host.py``'s own
``_default_cli_mode`` established) -- the trap
``tests/test_cli_worker.py``'s docstring documents hitting for real.
"""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest
from _pytest.monkeypatch import MonkeyPatch
from typer.testing import CliRunner

import clio_relay.relay_ops as relay_ops
from clio_relay.cli import app
from clio_relay.cluster_config import ClusterDefinition, ClusterRegistry
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.models import JarvisRunSpec, JobKind, JobState, JobWaitResult, RelayJob
from tests.test_cli import _write_test_cluster


@pytest.fixture(autouse=True)
def _default_cli_mode(  # pyright: ignore[reportUnusedFunction]
    monkeypatch: MonkeyPatch, tmp_path: Path
) -> None:
    """Mirror ``test_cli.py``'s own autouse fixture's env-var half only."""
    monkeypatch.setenv("CLIO_RELAY_CLI_MODE", "local")
    monkeypatch.setenv(
        "CLIO_RELAY_INSTALL_RECEIPT",
        str(tmp_path / "relay-state" / "install-receipt.json"),
    )


def test_cli_job_wait_returns_current_state_when_observation_expires(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    core_dir = tmp_path / "core"
    queue = ClioCoreQueue(core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_name="long-cli-run"),
            idempotency_key="long-cli-run",
        )
    )
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(core_dir))
    observations: list[tuple[str, float, float]] = []

    def observe(
        selected_queue: ClioCoreQueue,
        job_id: str,
        *,
        timeout_seconds: float,
        poll_seconds: float,
    ) -> JobWaitResult:
        observations.append((job_id, timeout_seconds, poll_seconds))
        return relay_ops.job_wait_result(
            selected_queue.get_job(job_id),
            timeout_seconds=timeout_seconds,
        )

    monkeypatch.setattr(relay_ops, "observe_until_terminal", observe)
    result = CliRunner().invoke(
        app,
        [
            "job",
            "wait",
            job.job_id,
            "--timeout-seconds",
            "0.25",
            "--poll-seconds",
            "0.05",
        ],
    )

    assert result.exit_code == 0
    observed = json.loads(result.output)
    assert observed["job_id"] == job.job_id
    assert observed["state"] == "queued"
    assert observed["observation"] == {
        "outcome": "observation_unknown",
        "timeout_seconds": 0.25,
        "scheduler_action": "none",
        "relay_action": "none",
    }
    assert observations == [(job.job_id, 0.25, 0.05)]
    assert queue.get_job(job.job_id).state is JobState.QUEUED


@pytest.mark.parametrize(
    ("option", "value"),
    [("--timeout-seconds", "inf"), ("--poll-seconds", "inf")],
)
def test_cli_job_wait_rejects_nonfinite_observation_bounds(
    option: str,
    value: str,
) -> None:
    result = CliRunner().invoke(
        app,
        ["job", "wait", "job_00000000000000000000000000000001", option, value],
    )

    assert result.exit_code != 0
    assert "positive and finite" in result.output


def test_cli_job_submit_can_request_exclusive_scheduler(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    core_dir = tmp_path / "core"
    yaml_path = tmp_path / "pipeline.yaml"
    yaml_path.write_text("name: generic\npkgs: []\n", encoding="utf-8")
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(core_dir))
    monkeypatch.setenv("CLIO_RELAY_CLI_MODE", "local")
    _write_test_cluster(tmp_path, name="test-cluster", scheduler_provider="slurm")
    monkeypatch.chdir(tmp_path)

    result = CliRunner().invoke(
        app,
        [
            "job",
            "submit",
            "--cluster",
            "test-cluster",
            "--jarvis-yaml",
            str(yaml_path),
            "--exclusive",
        ],
    )

    assert result.exit_code == 0
    job = ClioCoreQueue(core_dir).list_jobs()[0]
    assert isinstance(job.spec, JarvisRunSpec)
    assert "exclusive: true" in str(job.spec.pipeline_yaml)


def test_cli_job_submit_pipeline_creates_named_jarvis_job(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    core_dir = tmp_path / "core"
    monkeypatch.chdir(tmp_path)
    _write_test_cluster(tmp_path)
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(core_dir))

    result = CliRunner().invoke(
        app,
        [
            "job",
            "submit-pipeline",
            "--cluster",
            "ares",
            "--pipeline-name",
            "site_simulation_4node",
            "--idempotency-key",
            "named-pipeline",
        ],
    )

    assert result.exit_code == 0
    job = ClioCoreQueue(core_dir).get_job(result.output.strip())
    assert isinstance(job.spec, JarvisRunSpec)
    assert job.spec.pipeline_name == "site_simulation_4node"
    assert job.spec.pipeline_yaml is None


def test_cli_remote_job_submit_stages_yaml_and_uses_cluster_core(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("CLIO_RELAY_CLI_MODE", "ssh")
    ClusterRegistry(
        clusters={
            "ares": ClusterDefinition(
                name="ares",
                ssh_host="test-host",
                core_dir="/remote/core",
                spool_dir="/remote/spool",
            )
        }
    ).save(tmp_path / ".clio-relay" / "clusters.json")
    (tmp_path / "input.in").write_text("run 150\n", encoding="utf-8")
    yaml_path = tmp_path / "pipeline.yaml"
    yaml_path.write_text(
        """
name: remote-submit
x_clio_relay:
  stage_files:
    - local_path: input.in
      remote_path: .local/share/clio-relay/live-tests/{run_id}/input.in
pkgs:
  - pkg_type: site.simulation
    input: $HOME/.local/share/clio-relay/live-tests/{run_id}/input.in
""".lstrip(),
        encoding="utf-8",
    )
    writes: dict[str, bytes] = {}
    commands: list[list[str]] = []

    def fake_run(
        command: list[str],
        *,
        input: bytes | None = None,
        capture_output: bool,
        check: bool,
    ) -> subprocess.CompletedProcess[bytes]:
        commands.append(command)
        assert capture_output is True
        assert check is False
        if "cat > " in command[2]:
            remote_path = command[2].split("cat > ", maxsplit=1)[1].split(" &&", maxsplit=1)[0]
            writes[remote_path.strip("'")] = input or b""
            return subprocess.CompletedProcess(command, 0, b"", b"")
        if '"$HOME/.local/bin/clio-relay" job submit' in command[2]:
            assert "CLIO_RELAY_CLI_MODE=local" in command[2]
            assert 'CLIO_RELAY_CORE_DIR="/remote/core"' in command[2]
            return subprocess.CompletedProcess(command, 0, b"job_remote\n", b"")
        return subprocess.CompletedProcess(command, 0, b"", b"")

    monkeypatch.setattr("clio_relay.remote_cli.subprocess.run", fake_run)

    result = CliRunner().invoke(
        app,
        [
            "job",
            "submit",
            "--cluster",
            "ares",
            "--jarvis-yaml",
            str(yaml_path),
            "--idempotency-key",
            "desktop-submit",
        ],
    )

    assert result.exit_code == 0
    assert result.output.strip() == "job_remote"
    assert ClioCoreQueue(tmp_path / ".clio-relay" / "core").list_jobs() == []
    assert any(path.endswith("/input.in") for path in writes)
    staged_yaml = next(
        data.decode("utf-8") for path, data in writes.items() if path.endswith("/pipeline.yaml")
    )
    assert "x_clio_relay" not in staged_yaml
    assert ".local/share/clio-relay/live-tests/pipeline-" in staged_yaml
    assert any('"$HOME/.local/bin/clio-relay" job submit' in command[2] for command in commands)


def test_cli_remote_wait_passthrough_uses_cluster_core(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("CLIO_RELAY_CLI_MODE", "ssh")
    _write_test_cluster(tmp_path)
    commands: list[list[str]] = []
    remote_job = RelayJob(
        job_id="job_00000000000000000000000000000001",
        cluster="ares",
        kind=JobKind.JARVIS,
        state=JobState.QUEUED,
        spec=JarvisRunSpec(pipeline_name="long-remote-run"),
        idempotency_key="long-remote-run",
    )
    wait_result = relay_ops.job_wait_result(remote_job, timeout_seconds=1.0)

    def fake_run(
        command: list[str],
        *,
        capture_output: bool,
        check: bool,
        timeout: float | None = None,
    ) -> subprocess.CompletedProcess[bytes]:
        commands.append(command)
        assert capture_output is True
        assert check is False
        assert timeout == 11.0
        return subprocess.CompletedProcess(
            command,
            0,
            wait_result.model_dump_json().encode("utf-8"),
            b"",
        )

    monkeypatch.setattr("clio_relay.remote_cli.subprocess.run", fake_run)

    result = CliRunner().invoke(
        app,
        [
            "job",
            "wait",
            remote_job.job_id,
            "--cluster",
            "ares",
            "--timeout-seconds",
            "1",
            "--poll-seconds",
            "0.1",
        ],
    )

    assert result.exit_code == 0
    observed = json.loads(result.output)
    assert observed["job_id"] == remote_job.job_id
    assert observed["observation"]["outcome"] == "observation_unknown"
    assert len(commands) == 1
    assert f'"$HOME/.local/bin/clio-relay" job wait {remote_job.job_id}' in commands[0][2]


def test_cli_remote_wait_transport_expiry_reobserves_exact_status(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("CLIO_RELAY_CLI_MODE", "ssh")
    _write_test_cluster(tmp_path)
    remote_job = RelayJob(
        job_id="job_00000000000000000000000000000002",
        cluster="ares",
        kind=JobKind.JARVIS,
        state=JobState.QUEUED,
        spec=JarvisRunSpec(pipeline_name="long-remote-run"),
        idempotency_key="long-remote-run-timeout",
    )
    commands: list[list[str]] = []
    timeouts: list[float | None] = []

    def fake_run(
        command: list[str],
        *,
        capture_output: bool,
        check: bool,
        timeout: float | None = None,
    ) -> subprocess.CompletedProcess[bytes]:
        commands.append(command)
        timeouts.append(timeout)
        assert capture_output is True
        assert check is False
        if '"$HOME/.local/bin/clio-relay" job wait' in command[2]:
            raise subprocess.TimeoutExpired(command, timeout or 0)
        if '"$HOME/.local/bin/clio-relay" job status' in command[2]:
            return subprocess.CompletedProcess(
                command,
                0,
                json.dumps(
                    {
                        "job": remote_job.model_dump(mode="json"),
                        "relay_queue": {"state": "queued"},
                        "scheduler": [{"scheduler_job_id": "42", "raw_state": "PENDING"}],
                        "terminal": False,
                    }
                ).encode("utf-8"),
                b"",
            )
        raise AssertionError(f"unexpected command: {command}")

    monkeypatch.setattr("clio_relay.remote_cli.subprocess.run", fake_run)
    result = CliRunner().invoke(
        app,
        [
            "job",
            "wait",
            remote_job.job_id,
            "--cluster",
            "ares",
            "--timeout-seconds",
            "1",
            "--poll-seconds",
            "0.1",
        ],
    )

    assert result.exit_code == 0
    observed = json.loads(result.output)
    assert observed["job_id"] == remote_job.job_id
    assert observed["state"] == "queued"
    assert observed["observation"]["outcome"] == "observation_unknown"
    assert observed["observation"]["scheduler_action"] == "none"
    assert len(commands) == 2
    assert timeouts == [11.0, 30.0]


def test_cli_remote_wait_rejects_contradictory_terminal_claim(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("CLIO_RELAY_CLI_MODE", "ssh")
    _write_test_cluster(tmp_path)
    remote_job = RelayJob(
        job_id="job_00000000000000000000000000000003",
        cluster="ares",
        kind=JobKind.JARVIS,
        state=JobState.QUEUED,
        spec=JarvisRunSpec(pipeline_name="hostile-remote-run"),
        idempotency_key="hostile-remote-run",
    )

    def fake_run(
        command: list[str],
        *,
        capture_output: bool,
        check: bool,
        timeout: float | None = None,
    ) -> subprocess.CompletedProcess[bytes]:
        assert capture_output is True
        assert check is False
        assert timeout == 11.0
        contradictory = {
            **remote_job.model_dump(mode="json"),
            "observation": {
                "outcome": "terminal",
                "timeout_seconds": 1,
                "scheduler_action": "none",
                "relay_action": "none",
            },
        }
        return subprocess.CompletedProcess(
            command,
            0,
            json.dumps(contradictory).encode("utf-8"),
            b"",
        )

    monkeypatch.setattr("clio_relay.remote_cli.subprocess.run", fake_run)
    result = CliRunner().invoke(
        app,
        [
            "job",
            "wait",
            remote_job.job_id,
            "--cluster",
            "ares",
            "--timeout-seconds",
            "1",
        ],
    )

    assert result.exit_code == 1
    assert "remote job wait returned an invalid result" in result.output
