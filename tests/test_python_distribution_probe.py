from __future__ import annotations

import os
import shutil
import sys
from collections.abc import Callable
from pathlib import Path
from typing import cast

import clio_relay.python_distribution_probe as python_distribution_probe_module


def test_jarvis_launcher_matches_a_uv_managed_python_symlink(tmp_path: Path) -> None:
    """Bind JARVIS to the venv bin directory without following Python out of it."""
    environment_bin = tmp_path / "environment" / "bin"
    environment_bin.mkdir(parents=True)
    python = environment_bin / ("python.exe" if os.name == "nt" else "python")
    jarvis = environment_bin / ("jarvis.exe" if os.name == "nt" else "jarvis")
    if os.name == "nt":
        shutil.copy2(sys.executable, python)
        shutil.copy2(sys.executable, jarvis)
        executable = jarvis
    else:
        managed_python = tmp_path / "uv" / "python" / "bin" / "python3.12"
        managed_python.parent.mkdir(parents=True)
        managed_python.symlink_to(Path(sys.executable).resolve())
        python.symlink_to(managed_python)
        jarvis.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
        jarvis.chmod(0o755)
        external_bin = tmp_path / "external-bin"
        external_bin.mkdir()
        executable = external_bin / "jarvis"
        executable.symlink_to(jarvis)
        assert python.resolve().parent != environment_bin.resolve()

    matcher_name = "_jarvis_executable_matches_interpreter"
    matcher = cast(Callable[..., bool], getattr(python_distribution_probe_module, matcher_name))

    assert matcher(str(executable), str(python), runtime_command=[str(executable), "--help"])

    other = tmp_path / "other-bin" / executable.name
    other.parent.mkdir()
    if os.name == "nt":
        shutil.copy2(sys.executable, other)
    else:
        other.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
        other.chmod(0o755)
    assert not matcher(str(other), str(python), runtime_command=[str(other), "--help"])
