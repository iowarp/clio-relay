from __future__ import annotations

import os
import subprocess
from pathlib import Path

from _pytest.monkeypatch import MonkeyPatch

from clio_relay.config import RelaySettings


def test_settings_discover_bootstrap_managed_bins(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    home = tmp_path / "home"
    bin_dir = home / ".local" / "bin"
    bin_dir.mkdir(parents=True)
    jarvis = bin_dir / "jarvis"
    frpc = bin_dir / "frpc"
    jarvis.write_text("#!/bin/sh\n", encoding="utf-8")
    frpc.write_text("#!/bin/sh\n", encoding="utf-8")
    monkeypatch.setattr(Path, "home", lambda: home)
    monkeypatch.setenv("PATH", str(tmp_path / "empty-path"))
    monkeypatch.delenv("CLIO_RELAY_JARVIS_BIN", raising=False)
    monkeypatch.delenv("CLIO_RELAY_FRPC_BIN", raising=False)

    def always_usable(_name: str, _path: Path) -> bool:
        return True

    monkeypatch.setattr("clio_relay.config._candidate_is_usable", always_usable)

    settings = RelaySettings.from_env()

    assert settings.jarvis_bin == str(jarvis)
    assert settings.frpc_bin == str(frpc)


def test_settings_discover_bootstrap_managed_data_dirs(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    home = tmp_path / "home"
    core_dir = home / ".local" / "share" / "clio-relay" / "core"
    spool_dir = home / ".local" / "share" / "clio-relay" / "spool"
    core_dir.mkdir(parents=True)
    spool_dir.mkdir(parents=True)
    monkeypatch.setattr(Path, "home", lambda: home)
    monkeypatch.delenv("CLIO_RELAY_CORE_DIR", raising=False)
    monkeypatch.delenv("CLIO_RELAY_SPOOL_DIR", raising=False)

    settings = RelaySettings.from_env()

    assert settings.core_dir == core_dir.resolve()
    assert settings.spool_dir == spool_dir.resolve()


def test_settings_env_data_dirs_are_absolute(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", "relative-core")
    monkeypatch.setenv("CLIO_RELAY_SPOOL_DIR", "relative-spool")

    settings = RelaySettings.from_env()

    assert settings.core_dir == (tmp_path / "relative-core").resolve()
    assert settings.spool_dir == (tmp_path / "relative-spool").resolve()


def test_settings_env_bins_override_bootstrap_managed_bins(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.setattr(Path, "home", lambda: tmp_path / "home")
    monkeypatch.setenv("CLIO_RELAY_JARVIS_BIN", "/opt/jarvis")
    monkeypatch.setenv("CLIO_RELAY_FRPC_BIN", "/opt/frpc")

    settings = RelaySettings.from_env()

    assert settings.jarvis_bin == "/opt/jarvis"
    assert settings.frpc_bin == "/opt/frpc"


def test_settings_agent_defaults_are_provider_neutral(monkeypatch: MonkeyPatch) -> None:
    monkeypatch.delenv("CLIO_RELAY_AGENT_BIN", raising=False)
    monkeypatch.delenv("CLIO_RELAY_AGENT_ADAPTER", raising=False)

    settings = RelaySettings.from_env()

    assert settings.agent_bin == "agent"
    assert settings.agent_adapter == "exec"


def test_settings_agent_env_overrides(monkeypatch: MonkeyPatch) -> None:
    monkeypatch.setenv("CLIO_RELAY_AGENT_BIN", "/opt/agents/codex")
    monkeypatch.setenv("CLIO_RELAY_AGENT_ADAPTER", "codex")

    settings = RelaySettings.from_env()

    assert settings.agent_bin == "/opt/agents/codex"
    assert settings.agent_adapter == "codex"


def test_settings_discovers_project_local_frp_bins(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    bin_dir = tmp_path / ".tools" / "frp" / "bin"
    bin_dir.mkdir(parents=True)
    frpc = bin_dir / ("frpc.exe" if os.name == "nt" else "frpc")
    frpc.write_text("#!/bin/sh\n", encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("PATH", str(tmp_path / "empty-path"))
    monkeypatch.delenv("CLIO_RELAY_FRPC_BIN", raising=False)

    def always_usable(_name: str, _path: Path) -> bool:
        return True

    monkeypatch.setattr("clio_relay.config._candidate_is_usable", always_usable)

    settings = RelaySettings.from_env()

    assert settings.frpc_bin == str(frpc.resolve())


def test_settings_skips_broken_path_frp_shim_for_project_local_bin(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    home = tmp_path / "home"
    bin_dir = tmp_path / ".tools" / "frp" / "bin"
    bin_dir.mkdir(parents=True)
    broken = tmp_path / "broken-frpc.exe"
    local = bin_dir / ("frpc.exe" if os.name == "nt" else "frpc")
    broken.write_text("broken\n", encoding="utf-8")
    local.write_text("local\n", encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(Path, "home", lambda: home)
    monkeypatch.delenv("CLIO_RELAY_FRPC_BIN", raising=False)

    def fake_which(name: str) -> str | None:
        return str(broken) if name == "frpc" else None

    monkeypatch.setattr("clio_relay.config.shutil.which", fake_which)

    def fake_run(
        command: list[str],
        **_kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(
            command,
            0 if Path(command[0]) == local else 1,
            stdout="0.69.1",
            stderr="",
        )

    monkeypatch.setattr("clio_relay.config.subprocess.run", fake_run)

    settings = RelaySettings.from_env()

    assert settings.frpc_bin == str(local.resolve())
