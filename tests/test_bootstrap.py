from __future__ import annotations

import subprocess
import tarfile
from pathlib import Path
from typing import Any

import pytest

from clio_relay import __version__
from clio_relay.bootstrap import (
    assert_clean_git_checkout,
    create_bootstrap_archive,
    install_cluster_app_over_ssh,
    render_cluster_app_install_script,
    render_linux_user_bootstrap_script,
)
from clio_relay.errors import ConfigurationError


def test_linux_user_bootstrap_script_installs_required_components() -> None:
    script = render_linux_user_bootstrap_script(frp_version="0.69.1")

    assert 'FRP_VERSION="0.69.1"' in script
    assert 'ARCHIVE="frp_${FRP_VERSION}_linux_amd64.tar.gz"' in script
    assert "uv python install 3.12" in script
    assert 'uv venv --python 3.12 --seed --clear "$JARVIS_VENV"' in script
    assert "python3 -m venv" not in script
    assert "CLIO_RELAY_AGENT_NPM_PACKAGE" in script
    assert "CLIO_RELAY_AGENT_NPM_BIN" in script
    assert 'npm install -g "$AGENT_NPM_PACKAGE"' in script
    assert "CLIO_RELAY_AGENT_BIN" in script
    assert "AGENT_NPM_PACKAGE=${CLIO_RELAY_AGENT_NPM_PACKAGE:-''}" in script
    assert "AGENT_NPM_BIN=${CLIO_RELAY_AGENT_NPM_BIN:-''}" in script
    assert "CLIO_RELAY_AGENT_ADAPTER=exec" in script
    assert "CLIO_RELAY_AGENT_ARGS=''" in script
    assert "github.com/grc-iit/jarvis-cd.git" in script
    assert 'uv pip install --refresh-package clio-relay "$DEST"' in script
    assert 'jarvis repo add "$DEST/jarvis-packages/clio_relay" --force true' in script
    assert "spack install lammps" not in script
    assert 'cat > "$HOME/.local/bin/lmp"' not in script
    assert 'cat > "$HOME/.local/bin/mpiexec"' in script
    assert 'echo "mpich 4.0.0 clio-relay user-space wrapper"' in script
    assert "-p|-f|--host|--hostfile|-host|-hostfile|--hosts|-hosts|--ppn|-ppn|-npernode)" in script
    assert "-genv|--env)" in script
    assert "-env)" in script
    assert "*=*)" in script
    assert "[0-9]*)" in script
    assert 'if [ "${ranks:-1}" = "1" ]; then' in script
    assert 'exec srun -n "$ranks" "$@"' in script
    assert "CLIO_RELAY_CORE_DIR" in script
    assert "clio-relay init" in script
    assert "\r" not in script


def test_linux_user_bootstrap_script_accepts_explicit_npm_agent() -> None:
    script = render_linux_user_bootstrap_script(
        agent_adapter="codex",
        agent_npm_package="@openai/codex",
        agent_npm_bin="codex",
        agent_args=["--model", "gpt-5-codex"],
    )

    assert "AGENT_NPM_PACKAGE=${CLIO_RELAY_AGENT_NPM_PACKAGE:-@openai/codex}" in script
    assert "AGENT_NPM_BIN=${CLIO_RELAY_AGENT_NPM_BIN:-codex}" in script
    assert 'AGENT_BIN="$HOME/.local/bin/$AGENT_NPM_BIN"' in script
    assert "CLIO_RELAY_AGENT_ADAPTER=codex" in script
    assert "CLIO_RELAY_AGENT_ARGS='--model gpt-5-codex'" in script


def test_lammps_install_is_explicit_cluster_app_setup() -> None:
    script = render_cluster_app_install_script(app_name="lammps")

    assert "github.com/spack/spack.git" in script
    assert "spack install lammps" in script
    assert "spack load lammps" in script
    assert 'cat > "$HOME/.local/bin/lmp"' in script
    assert 'CLIO_RELAY_LAMMPS_BIN="$LAMMPS_BIN"' in script
    assert "\r" not in script


def test_cluster_app_install_rejects_unknown_app() -> None:
    with pytest.raises(ConfigurationError, match="unsupported cluster app"):
        render_cluster_app_install_script(app_name="vasp")


def test_cluster_app_install_sends_lf_script_bytes(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict[str, Any]] = []

    def fake_run(*args: Any, **kwargs: Any) -> subprocess.CompletedProcess[bytes]:
        del args
        calls.append(kwargs)
        return subprocess.CompletedProcess(
            ["ssh", "host", "bash", "-s"],
            0,
            stdout=b"ok\n",
            stderr=b"",
        )

    monkeypatch.setattr(subprocess, "run", fake_run)

    result = install_cluster_app_over_ssh(ssh_host="host", app_name="lammps")

    assert result == ["ok"]
    script = calls[0]["input"]
    assert isinstance(script, bytes)
    assert b"\r" not in script
    assert calls[0]["capture_output"] is True


def test_bootstrap_runner_decodes_remote_output_as_utf8(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, Any]] = []

    def fake_run(*args: Any, **kwargs: Any) -> subprocess.CompletedProcess[str]:
        del args
        calls.append(kwargs)
        return subprocess.CompletedProcess(["ssh", "host"], 0, stdout="ok", stderr="")

    monkeypatch.setattr(subprocess, "run", fake_run)

    from clio_relay import bootstrap

    result = bootstrap._run(["ssh", "host"])  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001

    assert result.stdout == "ok"
    assert calls[0]["encoding"] == "utf-8"
    assert calls[0]["errors"] == "replace"


def test_bootstrap_refuses_dirty_git_checkout(tmp_path: Path) -> None:
    subprocess.run(["git", "init"], cwd=tmp_path, check=True, capture_output=True)
    (tmp_path / "tracked.txt").write_text("clean\n", encoding="utf-8")
    subprocess.run(["git", "add", "tracked.txt"], cwd=tmp_path, check=True, capture_output=True)
    subprocess.run(
        [
            "git",
            "-c",
            "user.name=Test",
            "-c",
            "user.email=test@example.com",
            "commit",
            "-m",
            "init",
        ],
        cwd=tmp_path,
        check=True,
        capture_output=True,
    )
    (tmp_path / "tracked.txt").write_text("dirty\n", encoding="utf-8")

    with pytest.raises(ConfigurationError, match="deploys git HEAD"):
        assert_clean_git_checkout(tmp_path)


def test_bootstrap_archive_uses_clean_git_checkout(tmp_path: Path) -> None:
    subprocess.run(["git", "init"], cwd=tmp_path, check=True, capture_output=True)
    (tmp_path / "tracked.txt").write_text("clean\n", encoding="utf-8")
    (tmp_path / "jarvis-packages" / "clio_relay").mkdir(parents=True)
    (tmp_path / "jarvis-packages" / "clio_relay" / "README.md").write_text(
        "package\n",
        encoding="utf-8",
    )
    subprocess.run(["git", "add", "."], cwd=tmp_path, check=True, capture_output=True)
    subprocess.run(
        [
            "git",
            "-c",
            "user.name=Test",
            "-c",
            "user.email=test@example.com",
            "commit",
            "-m",
            "init",
        ],
        cwd=tmp_path,
        check=True,
        capture_output=True,
    )

    deployment = create_bootstrap_archive(
        source_root=tmp_path,
        archive=tmp_path / "bootstrap.tar",
    )

    assert deployment.install_spec == "$DEST"
    with tarfile.open(deployment.archive) as archive:
        names = archive.getnames()
    assert "tracked.txt" in names
    assert "jarvis-packages/clio_relay/README.md" in names


def test_bootstrap_archive_uses_packaged_assets_without_git_checkout(tmp_path: Path) -> None:
    deployment = create_bootstrap_archive(
        source_root=tmp_path / "not-a-repo",
        archive=tmp_path / "bootstrap.tar",
    )

    assert deployment.install_spec == f"clio-relay=={__version__}"
    with tarfile.open(deployment.archive) as archive:
        names = archive.getnames()
    assert any(name.startswith("jarvis-packages/clio_relay/") for name in names)
    assert not any("__pycache__" in name or name.endswith(".pyc") for name in names)
