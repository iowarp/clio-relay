from __future__ import annotations

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
