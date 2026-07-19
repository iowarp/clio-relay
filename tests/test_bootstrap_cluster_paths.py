from __future__ import annotations

import json
import os
import shutil
import socket
import subprocess
import sys
from pathlib import Path, PurePosixPath
from types import SimpleNamespace
from typing import Any

import pytest
from typer.testing import CliRunner

from clio_relay import bootstrap, cli
from clio_relay.bootstrap import (
    DEFAULT_REMOTE_CORE_DIR,
    DEFAULT_REMOTE_SPOOL_DIR,
    BootstrapArchive,
    render_linux_user_bootstrap_script,
)
from clio_relay.cluster_config import ClusterDefinition, ClusterRegistry
from clio_relay.deployment import endpoint_user_service_name
from clio_relay.errors import ConfigurationError


def test_bootstrap_data_directory_defaults_remain_backward_compatible() -> None:
    """Default bootstrap state remains in the established remote user directories."""
    script = render_linux_user_bootstrap_script()

    assert f'CLIO_RELAY_CORE_DIR="{DEFAULT_REMOTE_CORE_DIR}" ' in script
    assert f'CLIO_RELAY_SPOOL_DIR="{DEFAULT_REMOTE_SPOOL_DIR}" ' in script


def test_bootstrap_renders_custom_data_directories_without_shell_evaluation() -> None:
    """Only the leading HOME token expands; configured suffixes remain literal data."""
    core_dir = "$HOME/relay state/core$(touch /tmp/not-executed)"
    spool_dir = "/srv/clio state/spool's"

    script = render_linux_user_bootstrap_script(core_dir=core_dir, spool_dir=spool_dir)

    assert 'CLIO_RELAY_CORE_DIR="$HOME/relay state/core\\$(touch /tmp/not-executed)" ' in script
    assert f'CLIO_RELAY_SPOOL_DIR="{spool_dir}" ' in script
    assert f'CLIO_RELAY_CORE_DIR="{DEFAULT_REMOTE_CORE_DIR}"' not in script
    assert f'CLIO_RELAY_SPOOL_DIR="{DEFAULT_REMOTE_SPOOL_DIR}"' not in script


def test_managed_bootstrap_fences_worker_around_migration() -> None:
    """A configured worker is stopped before replacement and restarted after migration."""
    cluster = "Operator Target"
    service_name = endpoint_user_service_name(cluster)

    script = render_linux_user_bootstrap_script(cluster=cluster)

    stop = 'systemctl --user stop "$WORKER_SERVICE_NAME"'
    writer_proof = 'if ! python3 - "$WORKER_CLUSTER_NAME"'
    install = 'install -m 0755 "frp_${FRP_VERSION}_linux_amd64/frpc"'
    relay_replace = "uv tool install --force --python 3.12 --no-config"
    migrate = "clio-relay init --migrate-legacy-output"
    restart = 'systemctl --user start "$WORKER_SERVICE_NAME"'
    first_proof = script.index(writer_proof)
    second_proof = script.index(writer_proof, first_proof + 1)
    relay_replacement = script.rindex(relay_replace)
    assert f"WORKER_SERVICE_NAME={service_name}" in script
    assert script.index(stop) < first_proof < script.index(install)
    assert script.index(install) < relay_replacement < second_proof
    assert (
        second_proof
        < script.index(migrate)
        < script.rindex("if ! bootstrap_bounded_worker_restart; then")
    )
    assert restart in script
    assert "WORKER_WRITER_PROOF=1" in script
    assert 'exec 9>"$HOME/.local/share/clio-relay/bootstrap.lock"' in script
    assert "if ! flock -n 9; then" in script
    assert 'exec 8<>"$WORKER_LIFETIME_LOCK_PATH"' in script
    assert "WORKER_LIFETIME_GUARD_FD=8" in script
    assert 'exec 9<>"$WORKER_LIFETIME_LOCK_PATH"' not in script
    assert "bootstrap_bounded_worker_restart" in script
    assert "worker_recovery=restored" in script
    assert "worker state is unknown and requires operator verification" in script


def test_managed_bootstrap_releases_lifetime_guard_before_normal_restart() -> None:
    """The restarted worker can acquire its shared lifetime lock without timing out."""
    bash = shutil.which("bash")
    if bash is None:
        pytest.fail("bash is required to validate the Linux bootstrap restart path")
    worker_fence, worker_recheck, _init, worker_restart = bootstrap._worker_upgrade_fence_script(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        "test-cluster",
        rendered_core_dir='"$test_root/core"',
    )
    harness = f"""set -euo pipefail
test_root="$(mktemp -d)"
trap 'rm -rf -- "$test_root"' EXIT
mkdir -p "$test_root/bin"
export BOOTSTRAP_TEST_STATE="$test_root/worker-state"
echo active > "$BOOTSTRAP_TEST_STATE"
cat > "$test_root/bin/python3" <<'__FAKE_PYTHON__'
#!/usr/bin/env bash
cat >/dev/null
__FAKE_PYTHON__
cat > "$test_root/bin/systemctl" <<'__FAKE_SYSTEMCTL__'
#!/usr/bin/env bash
set -u
case "${{2:-}}" in
  show)
    case " $* " in
      *" --property=LoadState "*) echo loaded ;;
      *" --property=ActiveState "*) cat "$BOOTSTRAP_TEST_STATE" ;;
      *) exit 2 ;;
    esac
    ;;
  stop)
    echo "fake-systemctl=stop" >&2
    echo inactive > "$BOOTSTRAP_TEST_STATE"
    ;;
  start)
    echo "fake-systemctl=start" >&2
    if {{ true <&8; }} 2>/dev/null; then
      echo "fake-systemctl=start-with-lifetime-guard" >&2
      echo activating > "$BOOTSTRAP_TEST_STATE"
      exit 99
    fi
    echo active > "$BOOTSTRAP_TEST_STATE"
    ;;
  *) exit 2 ;;
esac
__FAKE_SYSTEMCTL__
chmod +x "$test_root/bin/python3" "$test_root/bin/systemctl"
export PATH="$test_root/bin:$PATH"
{worker_fence}
{worker_recheck}
{worker_restart}
"""

    result = subprocess.run(
        [bash, "-s"],
        input=harness.encode("utf-8"),
        capture_output=True,
        check=False,
        timeout=30,
    )
    stderr = result.stderr.decode("utf-8", errors="replace")

    assert result.returncode == 0, stderr
    assert stderr.count("fake-systemctl=stop") == 1
    assert stderr.count("fake-systemctl=start") == 1
    assert "fake-systemctl=start-with-lifetime-guard" not in stderr


@pytest.mark.parametrize("restart_succeeds", [True, False])
def test_failed_managed_bootstrap_restores_previously_active_worker(
    restart_succeeds: bool,
) -> None:
    """A sabotaged post-stop step preserves its error and boundedly restores the worker."""
    bash = shutil.which("bash")
    if bash is None:
        pytest.fail("bash is required to validate the Linux bootstrap recovery trap")
    worker_fence, _recheck, _init, _restart = bootstrap._worker_upgrade_fence_script(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        "ares",
        rendered_core_dir='"$test_root/core"',
    )
    restart_result = (
        'echo active > "$BOOTSTRAP_TEST_STATE"\nexit 0' if restart_succeeds else "exit 1"
    )
    harness = f"""set -euo pipefail
test_root="$(mktemp -d)"
mkdir -p "$test_root/bin"
export BOOTSTRAP_TEST_STATE="$test_root/worker-state"
export BOOTSTRAP_TEST_STARTED="$test_root/start-attempted"
echo active > "$BOOTSTRAP_TEST_STATE"
cat > "$test_root/bin/python3" <<'__FAKE_PYTHON__'
#!/usr/bin/env bash
cat >/dev/null
__FAKE_PYTHON__
cat > "$test_root/bin/systemctl" <<'__FAKE_SYSTEMCTL__'
#!/usr/bin/env bash
set -u
case "${{2:-}}" in
  show)
    case " $* " in
      *" --property=LoadState "*) echo loaded ;;
      *" --property=ActiveState "*)
        state="$(cat "$BOOTSTRAP_TEST_STATE")"
        echo "$state"
        if [ -f "$BOOTSTRAP_TEST_STARTED" ]; then
          rm -rf -- "$(dirname "$BOOTSTRAP_TEST_STATE")"
        fi
        ;;
      *) exit 2 ;;
    esac
    ;;
  stop)
    echo "fake-systemctl=stop" >&2
    echo inactive > "$BOOTSTRAP_TEST_STATE"
    ;;
  start)
    echo "fake-systemctl=start" >&2
    if {{ true <&8; }} 2>/dev/null; then
      echo "fake-systemctl=start-with-lifetime-guard" >&2
      exit 99
    fi
    : > "$BOOTSTRAP_TEST_STARTED"
    {restart_result}
    ;;
  *) exit 2 ;;
esac
__FAKE_SYSTEMCTL__
chmod +x "$test_root/bin/python3" "$test_root/bin/systemctl"
export PATH="$test_root/bin:$PATH"
{worker_fence}
echo "remote-bootstrap-step=sabotaged" >&2
exit 37
"""

    result = subprocess.run(
        [bash, "-s"],
        input=harness.encode("utf-8"),
        capture_output=True,
        check=False,
        timeout=30,
    )
    stderr = result.stderr.decode("utf-8", errors="replace")

    assert result.returncode == 37
    assert "remote-bootstrap-step=sabotaged" in stderr
    assert stderr.count("fake-systemctl=stop") == 1
    assert "fake-systemctl=start" in stderr
    assert "fake-systemctl=start-with-lifetime-guard" not in stderr
    if restart_succeeds:
        assert stderr.count("fake-systemctl=start") == 1
        assert "worker_recovery=restored" in stderr
        assert "worker_recovery=failed" not in stderr
    else:
        assert stderr.count("fake-systemctl=start") == 1
        assert "worker_recovery=failed" in stderr
        assert "state=inactive" in stderr


def test_standalone_bootstrap_cannot_mutate_legacy_output() -> None:
    """Without a managed cluster identity bootstrap uses the fail-closed ordinary init path."""
    script = render_linux_user_bootstrap_script()

    assert "WORKER_SERVICE_NAME=''" in script
    assert "systemctl --user stop" not in script
    assert "clio-relay init --migrate-legacy-output" not in script
    assert "clio-relay init" in script


def _fake_proc_process(
    proc_root: Path,
    *,
    pid: int,
    argv: list[str],
    environment: dict[str, str],
    state: str = "S",
    start_ticks: int = 42,
) -> None:
    """Create bounded proc-like argv and environment fixtures for the embedded proof."""
    process = proc_root / str(pid)
    process.mkdir(parents=True)
    process.joinpath("cmdline").write_bytes(
        b"\0".join(os.fsencode(argument) for argument in argv) + b"\0"
    )
    process.joinpath("environ").write_bytes(
        b"\0".join(os.fsencode(f"{name}={value}") for name, value in environment.items()) + b"\0"
    )
    process.joinpath("stat").write_text(
        f"{pid} (python worker) {state} " + " ".join(["0"] * 18 + [str(start_ticks)]) + "\n",
        encoding="ascii",
    )
    boot_id = proc_root / "sys" / "kernel" / "random" / "boot_id"
    boot_id.parent.mkdir(parents=True, exist_ok=True)
    boot_id.write_text("test-boot\n", encoding="ascii")


def _write_worker_endpoint_record(
    core_dir: Path,
    *,
    endpoint_id: str,
    pid: int,
    cluster: str,
    process_identity: dict[str, object] | None = None,
) -> None:
    """Write one cross-version endpoint record used by the live-PID proof."""
    endpoint_dir = core_dir / "endpoints"
    endpoint_dir.mkdir(parents=True, exist_ok=True)
    metadata: dict[str, object] = {}
    if process_identity is not None:
        metadata["process_identity"] = process_identity
    document = {
        "endpoint_id": endpoint_id,
        "role": "worker",
        "cluster": cluster,
        "hostname": socket.gethostname(),
        "pid": pid,
        "registered_at": "2000-01-01T00:00:00Z",
        "last_seen_at": "2000-01-01T00:00:00Z",
        "metadata": metadata,
    }
    (endpoint_dir / f"{endpoint_id}.json").write_text(
        json.dumps(document),
        encoding="utf-8",
    )


def _fixture_uid() -> int:
    """Return a deterministic uid for cross-platform process identity fixtures."""
    getuid = getattr(os, "getuid", None)
    if not callable(getuid):
        return 0
    uid = getuid()
    return uid if isinstance(uid, int) else 0


def _run_writer_proof(
    proc_root: Path,
    *,
    cluster: str,
    core_dir: Path | PurePosixPath,
) -> subprocess.CompletedProcess[str]:
    """Execute the exact Python source embedded in managed bootstrap scripts."""
    return subprocess.run(
        [
            sys.executable,
            "-c",
            vars(bootstrap)["_WORKER_WRITER_PROOF_PYTHON"],
            cluster,
            str(core_dir),
            str(proc_root),
        ],
        capture_output=True,
        text=True,
        check=False,
    )


def test_embedded_writer_proof_rejects_exact_live_worker(
    tmp_path: Path,
) -> None:
    """Exact console argv and environment ownership stop migration."""
    proc_root = tmp_path / "proc"
    core_dir = tmp_path / "core"
    _fake_proc_process(
        proc_root,
        pid=1729,
        argv=[
            "/usr/bin/python3",
            "/home/operator/.local/bin/clio-relay",
            "endpoint",
            "start",
            "--role",
            "worker",
            "--cluster",
            "custom",
        ],
        environment={
            "HOME": str(tmp_path),
            "CLIO_RELAY_CORE_DIR": str(tmp_path / "unused" / ".." / "core"),
        },
    )

    result = _run_writer_proof(proc_root, cluster="custom", core_dir=core_dir)

    assert result.returncode != 0
    assert "live endpoint pid=1729" in result.stderr
    assert "cluster='custom'" in result.stderr


@pytest.mark.parametrize(
    ("argv", "writer_kind"),
    [
        (
            [
                "/home/operator/.local/bin/clio-relay",
                "api",
                "start",
                "--host",
                "127.0.0.1",
            ],
            "api",
        ),
        (
            [
                "/home/operator/.local/bin/clio-relay",
                "mcp-server",
                "--profile",
                "user",
            ],
            "mcp-server",
        ),
    ],
)
def test_embedded_writer_proof_rejects_every_same_core_long_lived_writer(
    tmp_path: Path,
    argv: list[str],
    writer_kind: str,
) -> None:
    """Retained cluster API and MCP processes cannot outlive an index migration."""
    proc_root = tmp_path / "proc"
    core_dir = tmp_path / "core"
    _fake_proc_process(
        proc_root,
        pid=1738,
        argv=argv,
        environment={"HOME": str(tmp_path), "CLIO_RELAY_CORE_DIR": str(core_dir)},
    )

    result = _run_writer_proof(proc_root, cluster="custom", core_dir=core_dir)

    assert result.returncode != 0
    assert f"live {writer_kind} writer pid=1738" in result.stderr
    assert "stop or detach it before bootstrap" in result.stderr


def test_embedded_writer_proof_allows_long_lived_writer_on_different_core(
    tmp_path: Path,
) -> None:
    """Long-lived relay processes remain isolated by physical core identity."""
    proc_root = tmp_path / "proc"
    _fake_proc_process(
        proc_root,
        pid=1739,
        argv=["/home/operator/.local/bin/clio-relay", "api", "start"],
        environment={
            "HOME": str(tmp_path),
            "CLIO_RELAY_CORE_DIR": str(tmp_path / "different-core"),
        },
    )

    result = _run_writer_proof(
        proc_root,
        cluster="custom",
        core_dir=tmp_path / "expected-core",
    )

    assert result.returncode == 0, result.stderr


def test_embedded_writer_proof_rejects_in_flight_same_core_cli_writer(
    tmp_path: Path,
) -> None:
    """A short-lived pre-upgrade CLI process cannot outlive final reconciliation."""
    proc_root = tmp_path / "proc"
    core_dir = tmp_path / "core"
    _fake_proc_process(
        proc_root,
        pid=1744,
        argv=[
            "/usr/bin/python3",
            "/home/operator/.local/bin/clio-relay",
            "job",
            "submit",
            "--cluster",
            "custom",
        ],
        environment={"HOME": str(tmp_path), "CLIO_RELAY_CORE_DIR": str(core_dir)},
    )

    result = _run_writer_proof(proc_root, cluster="custom", core_dir=core_dir)

    assert result.returncode != 0
    assert "live clio-relay process pid=1744" in result.stderr
    assert "wait for it to exit before bootstrap" in result.stderr


def test_embedded_writer_proof_does_not_claim_python_module_argv(tmp_path: Path) -> None:
    """A generic Python module shape is not treated as a proven console invocation."""
    proc_root = tmp_path / "proc"
    _fake_proc_process(
        proc_root,
        pid=1728,
        argv=[
            "/usr/bin/python3",
            "-m",
            "clio_relay.cli",
            "endpoint",
            "start",
            "--role=worker",
            "--cluster=custom",
        ],
        environment={"HOME": str(tmp_path), "CLIO_RELAY_CORE_DIR": str(tmp_path / "core")},
    )

    result = _run_writer_proof(
        proc_root,
        cluster="custom",
        core_dir=tmp_path / "core",
    )

    assert result.returncode == 0, result.stderr


def test_embedded_writer_proof_uses_legacy_endpoint_pid_for_python_wrapper(
    tmp_path: Path,
) -> None:
    """An exact-core legacy record blocks a live wrapper without recognizable argv."""
    proc_root = tmp_path / "proc"
    core_dir = tmp_path / "core"
    _fake_proc_process(
        proc_root,
        pid=1740,
        argv=["/usr/bin/python3", "-c", "run_worker()"],
        environment={"HOME": str(tmp_path)},
    )
    _write_worker_endpoint_record(
        core_dir,
        endpoint_id="legacy_wrapper",
        pid=1740,
        cluster="other",
    )

    result = _run_writer_proof(proc_root, cluster="custom", core_dir=core_dir)

    assert result.returncode != 0
    assert "live endpoint pid=1740 has exact-core record" in result.stderr


def test_embedded_writer_proof_only_dismisses_well_formed_stale_identity(
    tmp_path: Path,
) -> None:
    """Exact generation mismatch permits PID reuse; malformed identity remains conservative."""
    proc_root = tmp_path / "proc"
    stale_core = tmp_path / "stale-core"
    malformed_core = tmp_path / "malformed-core"
    _fake_proc_process(
        proc_root,
        pid=1741,
        argv=["/usr/bin/python3", "-c", "unrelated()"],
        environment={"HOME": str(tmp_path)},
        start_ticks=99,
    )
    _write_worker_endpoint_record(
        stale_core,
        endpoint_id="stale_generation",
        pid=1741,
        cluster="old",
        process_identity={
            "schema_version": "clio-relay.process-identity.v1",
            "boot_id": "test-boot",
            "start_ticks": 98,
            "uid": _fixture_uid(),
            "pid": 1741,
        },
    )
    _write_worker_endpoint_record(
        malformed_core,
        endpoint_id="malformed_generation",
        pid=1741,
        cluster="old",
        process_identity={"schema_version": "clio-relay.process-identity.v1"},
    )

    stale = _run_writer_proof(proc_root, cluster="custom", core_dir=stale_core)
    malformed = _run_writer_proof(proc_root, cluster="custom", core_dir=malformed_core)

    assert stale.returncode == 0, stale.stderr
    assert malformed.returncode != 0
    assert "live endpoint pid=1741" in malformed.stderr


def test_embedded_writer_proof_retains_duplicate_pid_evidence(tmp_path: Path) -> None:
    """A stale exact record cannot overwrite conservative legacy evidence for one PID."""
    proc_root = tmp_path / "proc"
    core_dir = tmp_path / "core"
    _fake_proc_process(
        proc_root,
        pid=1742,
        argv=[],
        environment={"HOME": str(tmp_path)},
        start_ticks=200,
    )
    _write_worker_endpoint_record(
        core_dir,
        endpoint_id="a_legacy",
        pid=1742,
        cluster="legacy",
    )
    _write_worker_endpoint_record(
        core_dir,
        endpoint_id="z_stale",
        pid=1742,
        cluster="stale",
        process_identity={
            "schema_version": "clio-relay.process-identity.v1",
            "boot_id": "test-boot",
            "start_ticks": 199,
            "uid": _fixture_uid(),
            "pid": 1742,
        },
    )

    result = _run_writer_proof(proc_root, cluster="custom", core_dir=core_dir)

    assert result.returncode != 0
    assert "cluster='legacy'" in result.stderr


def test_embedded_writer_proof_matches_empty_home_semantics(tmp_path: Path) -> None:
    """An explicitly empty HOME maps a tilde core to root like RelaySettings."""
    proc_root = tmp_path / "proc"
    _fake_proc_process(
        proc_root,
        pid=1743,
        argv=[
            "/home/operator/.local/bin/clio-relay",
            "endpoint",
            "start",
            "--role=worker",
            "--cluster=custom",
        ],
        environment={"HOME": "", "CLIO_RELAY_CORE_DIR": "~/core"},
    )

    result = _run_writer_proof(
        proc_root,
        cluster="custom",
        core_dir=PurePosixPath("/core"),
    )

    assert result.returncode != 0
    assert "live endpoint pid=1743" in result.stderr


def test_embedded_writer_proof_uses_exact_environment_fields(tmp_path: Path) -> None:
    """Substrings and unrelated environment fields cannot manufacture a core collision."""
    proc_root = tmp_path / "proc"
    expected_core = tmp_path / "expected-core"
    different_core = tmp_path / "different-core"
    _fake_proc_process(
        proc_root,
        pid=1730,
        argv=[
            "/home/operator/.local/bin/clio-relay",
            "endpoint",
            "start",
            "--role",
            "worker",
            "--cluster",
            "custom-suffix",
            "--label=clio-relay endpoint start --cluster custom",
        ],
        environment={
            "HOME": str(tmp_path),
            "CLIO_RELAY_CORE_DIR": str(different_core),
            "UNRELATED": f"CLIO_RELAY_CORE_DIR={expected_core}",
        },
    )
    _fake_proc_process(
        proc_root,
        pid=1732,
        argv=[
            "/home/operator/.local/bin/clio-relay",
            "endpoint",
            "start",
            "--role=worker",
            "--cluster=custom",
        ],
        environment={
            "HOME": str(tmp_path),
            "CLIO_RELAY_CORE_DIR": str(different_core),
            "UNRELATED": f"CLIO_RELAY_CORE_DIR={expected_core}",
        },
    )

    result = _run_writer_proof(proc_root, cluster="custom", core_dir=expected_core)

    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "relay_worker_writer_proof=clear"


def test_embedded_writer_proof_rejects_other_cluster_worker_on_same_core(
    tmp_path: Path,
) -> None:
    """Core ownership, not a cluster label, fences the core-wide migration."""
    proc_root = tmp_path / "proc"
    core_dir = tmp_path / "shared-core"
    _fake_proc_process(
        proc_root,
        pid=1733,
        argv=[
            "/home/operator/.local/bin/clio-relay",
            "endpoint",
            "start",
            "--role=worker",
            "--cluster=other-cluster",
        ],
        environment={
            "HOME": str(tmp_path),
            "CLIO_RELAY_CORE_DIR": str(core_dir),
        },
    )

    result = _run_writer_proof(proc_root, cluster="custom", core_dir=core_dir)

    assert result.returncode != 0
    assert "cluster='other-cluster'" in result.stderr
    assert "bootstrapping cluster='custom'" in result.stderr


def test_embedded_writer_proof_allows_other_cluster_worker_on_different_core(
    tmp_path: Path,
) -> None:
    """A different cluster remains harmless when it owns a different core queue."""
    proc_root = tmp_path / "proc"
    _fake_proc_process(
        proc_root,
        pid=1734,
        argv=[
            "/home/operator/.local/bin/clio-relay",
            "endpoint",
            "start",
            "--role",
            "worker",
            "--cluster",
            "other-cluster",
        ],
        environment={
            "HOME": str(tmp_path),
            "CLIO_RELAY_CORE_DIR": str(tmp_path / "different-core"),
        },
    )

    result = _run_writer_proof(
        proc_root,
        cluster="custom",
        core_dir=tmp_path / "expected-core",
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "relay_worker_writer_proof=clear"


def test_embedded_writer_proof_fails_closed_on_oversized_proc_value(tmp_path: Path) -> None:
    """The proof never accepts an argv value beyond its explicit read bound."""
    proc_root = tmp_path / "proc"
    process = proc_root / "1731"
    process.mkdir(parents=True)
    process.joinpath("cmdline").write_bytes(b"x" * (1_048_576 + 1))
    process.joinpath("environ").write_bytes(b"")

    result = _run_writer_proof(proc_root, cluster="custom", core_dir=tmp_path / "core")

    assert result.returncode != 0
    assert "exceeds the bounded inspection size" in result.stderr


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("core_dir", "relative/core"),
        ("spool_dir", "$USER/relay-spool"),
        ("core_dir", "$HOME/relay\ncore"),
    ],
)
def test_bootstrap_rejects_ambiguous_remote_data_directories(field: str, value: str) -> None:
    """Bootstrap refuses paths whose expansion or shell boundaries are ambiguous."""
    arguments = {
        "core_dir": DEFAULT_REMOTE_CORE_DIR,
        "spool_dir": DEFAULT_REMOTE_SPOOL_DIR,
    }
    arguments[field] = value

    with pytest.raises(ConfigurationError, match=field):
        render_linux_user_bootstrap_script(
            core_dir=arguments["core_dir"],
            spool_dir=arguments["spool_dir"],
        )


def test_bootstrap_over_ssh_forwards_configured_data_directories(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The SSH bootstrap carries both configured directories into script rendering."""
    captured: dict[str, object] = {}

    def fake_create_bootstrap_archive(
        *,
        source_root: Path,
        archive: Path,
        relay_wheel: Path | None,
    ) -> BootstrapArchive:
        assert source_root == tmp_path
        assert relay_wheel is None
        archive.write_bytes(b"archive")
        return BootstrapArchive(archive=archive, install_spec="clio-relay==1.0.0")

    def fake_render_linux_user_bootstrap_script(**kwargs: object) -> str:
        captured.update(kwargs)
        return "set -euo pipefail\n"

    receipt = {
        "schema_version": "clio-relay.bootstrap-receipt.v1",
        "invocation_id": "bootstrap_paths",
        "bootstrap_profile": "linux-user",
        "relay_install_spec": "clio-relay==1.0.0",
        "install_receipt_sha256": "a" * 64,
        "worker_fence": {
            "managed": True,
            "service_name": endpoint_user_service_name("custom"),
            "was_active": True,
            "writer_proof": True,
            "writer_recheck": True,
            "lifetime_exclusive": True,
            "restarted": True,
        },
        "completed_at": "2026-07-14T00:00:00Z",
    }

    def fake_run(command: list[str]) -> subprocess.CompletedProcess[str]:
        stdout = (
            json.dumps(receipt)
            if command[-2:] == ["cat", "$HOME/.local/share/clio-relay/bootstrap-receipt.json"]
            else ""
        )
        return subprocess.CompletedProcess(command, 0, stdout, "")

    monkeypatch.setattr(bootstrap, "create_bootstrap_archive", fake_create_bootstrap_archive)
    monkeypatch.setattr(
        bootstrap,
        "render_linux_user_bootstrap_script",
        fake_render_linux_user_bootstrap_script,
    )
    monkeypatch.setattr(bootstrap, "_run", fake_run)
    monkeypatch.setattr(bootstrap, "uuid4", lambda: SimpleNamespace(hex="paths"))

    def fake_which(executable: str) -> str:
        return executable

    monkeypatch.setattr(bootstrap.shutil, "which", fake_which)

    bootstrap.bootstrap_cluster_over_ssh(
        bootstrap_profile="linux-user",
        ssh_host="cluster.example.test",
        source_root=tmp_path,
        cluster="custom",
        core_dir="$HOME/custom/core",
        spool_dir="/srv/custom/spool",
    )

    assert captured["cluster"] == "custom"
    assert captured["core_dir"] == "$HOME/custom/core"
    assert captured["spool_dir"] == "/srv/custom/spool"


def test_cluster_bootstrap_cli_uses_configured_data_directories(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The cluster command propagates its registry's queue and spool locations."""
    monkeypatch.chdir(tmp_path)
    ClusterRegistry(
        clusters={
            "custom": ClusterDefinition(
                name="custom",
                ssh_host="cluster.example.test",
                core_dir="$HOME/operator core",
                spool_dir="/srv/operator spool",
            )
        }
    ).save(tmp_path / ".clio-relay" / "clusters.json")
    wheel = tmp_path / "clio_relay-1.0.0-py3-none-any.whl"
    wheel.write_bytes(b"wheel")
    captured: dict[str, object] = {}

    def fake_bootstrap_cluster_over_ssh(**kwargs: object) -> list[str]:
        captured.update(kwargs)
        receipt = {
            "schema_version": "clio-relay.bootstrap-receipt.v1",
            "invocation_id": "bootstrap_custom_paths",
            "bootstrap_profile": "linux-user",
            "relay_install_spec": "clio-relay==1.0.0",
            "install_receipt_sha256": "b" * 64,
            "completed_at": "2026-07-14T00:00:00Z",
        }
        return [
            "bootstrap_receipt=/home/test/.local/share/clio-relay/bootstrap-receipt.json",
            "bootstrap_receipt_json=" + json.dumps(receipt, sort_keys=True),
        ]

    def fake_remote_target_identity(_definition: ClusterDefinition) -> dict[str, Any]:
        return {"verified": True}

    monkeypatch.setattr(cli, "package_source_root", lambda: tmp_path / "package")
    monkeypatch.setattr(cli, "bootstrap_cluster_over_ssh", fake_bootstrap_cluster_over_ssh)
    monkeypatch.setattr(cli, "_remote_target_identity", fake_remote_target_identity)

    result = CliRunner().invoke(
        cli.app,
        [
            "cluster",
            "bootstrap",
            "--cluster",
            "custom",
            "--relay-wheel",
            str(wheel),
        ],
    )

    assert result.exit_code == 0, result.output
    assert captured["cluster"] == "custom"
    assert captured["core_dir"] == "$HOME/operator core"
    assert captured["spool_dir"] == "/srv/operator spool"
