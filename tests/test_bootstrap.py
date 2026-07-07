from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from clio_relay.bootstrap import assert_clean_git_checkout, render_linux_user_bootstrap_script
from clio_relay.errors import ConfigurationError


def test_linux_user_bootstrap_script_installs_required_components() -> None:
    script = render_linux_user_bootstrap_script(frp_version="0.69.1")

    assert 'FRP_VERSION="0.69.1"' in script
    assert 'ARCHIVE="frp_${FRP_VERSION}_linux_amd64.tar.gz"' in script
    assert "uv python install 3.12" in script
    assert "CLIO_RELAY_AGENT_NPM_PACKAGE" in script
    assert "CLIO_RELAY_AGENT_NPM_BIN" in script
    assert 'npm install -g "$AGENT_NPM_PACKAGE"' in script
    assert "CLIO_RELAY_AGENT_BIN" in script
    assert "AGENT_NPM_PACKAGE=${CLIO_RELAY_AGENT_NPM_PACKAGE:-''}" in script
    assert "AGENT_NPM_BIN=${CLIO_RELAY_AGENT_NPM_BIN:-''}" in script
    assert "CLIO_RELAY_AGENT_ADAPTER=exec" in script
    assert "CLIO_RELAY_AGENT_ARGS=''" in script
    assert "github.com/grc-iit/jarvis-cd.git" in script
    assert 'jarvis repo add "$DEST/jarvis-packages/clio_relay" --force true' in script
    assert 'cat > "$HOME/.local/bin/lmp"' in script
    assert "spack install lammps" in script
    assert "spack load lammps" in script
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
