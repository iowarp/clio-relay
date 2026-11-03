"""Cold bootstrap refuses an unusable host with a message, not a traceback.

iowarp/clio-relay#283, found on a real Debian 11 host:

1. a system ``python3`` older than 3.10 crashed the candidate stage with
   ``TypeError: dataclass() got an unexpected keyword argument 'slots'``;
2. a stale ``~/.local/bin/uv`` failed the pin check without naming what was
   found or what was expected;
3. an absent ``~/.local/bin/uv`` crashed with a raw ``FileNotFoundError``.

These run the rendered script's own shell under a real bash against a
synthetic ``$HOME``.
"""

from __future__ import annotations

import hashlib
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

import clio_relay.bootstrap_script_host_requirements as host_requirements
from clio_relay.bootstrap import render_linux_user_bootstrap_script
from clio_relay.bootstrap_constants import UV_LINUX_AMD64_EXECUTABLE_SHA256, UV_VERSION

pytestmark = pytest.mark.skipif(
    sys.platform == "win32" or shutil.which("bash") is None or shutil.which("flock") is None,
    reason="runs the Linux bootstrap shell, which needs bash and flock",
)

_LOCK_STAGE = 'if [ "${CLIO_RELAY_BOOTSTRAP_LOCK_FD:-}" != 9 ]; then'


def _script_before_first_python_stage() -> str:
    """The rendered script up to its first executed python3 stage (the lock/re-exec)."""
    script = render_linux_user_bootstrap_script(frp_version="0.69.1")
    assert script.count(_LOCK_STAGE) == 1
    return script.split(_LOCK_STAGE, 1)[0]


def _executable(path: Path, body: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body, encoding="utf-8")
    path.chmod(0o700)
    return path


def _fake_python3(home: Path, *, version: str, supported: bool) -> None:
    _executable(
        home / ".local" / "bin" / "python3",
        f"#!/bin/sh\nprintf '%s' '{version}'\nexit {0 if supported else 1}\n",
    )


def _run(script: str, *, home: Path, path: str) -> subprocess.CompletedProcess[str]:
    bash = shutil.which("bash")
    assert bash is not None
    return subprocess.run(
        [bash, "-c", script],
        env={"HOME": str(home), "PATH": path},
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )


def _tools_only_path(tmp_path: Path) -> str:
    """A PATH holding just the tools the preamble runs before its python check."""
    tools = tmp_path / "tools"
    tools.mkdir()
    for name in ("mkdir", "flock"):
        found = shutil.which(name)
        assert found is not None
        (tools / name).symlink_to(found)
    return str(tools)


def test_old_system_python_is_refused_naming_version_path_and_fix(tmp_path: Path) -> None:
    home = tmp_path / "home"
    _fake_python3(home, version="3.9.2", supported=False)

    completed = _run(
        _script_before_first_python_stage(), home=home, path=_tools_only_path(tmp_path)
    )

    assert completed.returncode == 1
    assert "requires python3 >= 3.10" in completed.stderr
    assert f"found 3.9.2 at {home}/.local/bin/python3" in completed.stderr
    assert "$HOME/.local/bin is searched first" in completed.stderr
    assert "Traceback" not in completed.stderr


def test_missing_system_python_is_refused(tmp_path: Path) -> None:
    home = tmp_path / "home"
    home.mkdir()

    completed = _run(
        _script_before_first_python_stage(), home=home, path=_tools_only_path(tmp_path)
    )

    assert completed.returncode == 1
    assert "requires python3 >= 3.10" in completed.stderr
    assert "none was found" in completed.stderr


def test_supported_system_python_passes_the_floor_check(tmp_path: Path) -> None:
    home = tmp_path / "home"
    _fake_python3(home, version="3.12.1", supported=True)

    completed = _run(
        _script_before_first_python_stage(), home=home, path=_tools_only_path(tmp_path)
    )

    assert completed.returncode == 0, completed.stderr
    assert completed.stderr == ""


def test_floor_probe_reports_the_running_interpreter() -> None:
    """The probe the fakes stand in for really prints the version and passes on this Python."""
    script = host_requirements.host_requirements_script()
    probe = script.split('python3 -c "', 1)[1].split('" 2>/dev/null', 1)[0]

    completed = subprocess.run(
        [sys.executable, "-c", probe], capture_output=True, text=True, check=False
    )

    assert completed.returncode == 0
    assert completed.stdout == ".".join(str(part) for part in sys.version_info[:3])


def _require_uv(home: Path, *, executable_sha256: str | None = None) -> str:
    script = host_requirements.host_requirements_script()
    if executable_sha256 is not None:
        script = script.replace(UV_LINUX_AMD64_EXECUTABLE_SHA256, executable_sha256)
    call = f'bootstrap_require_pinned_uv_source "{home}/.local/bin/uv"'
    return f"set -euo pipefail\n{script}\n{call}\n"


def _path_with_supported_python(home: Path) -> str:
    _fake_python3(home, version="3.12.1", supported=True)
    return f"{home}/.local/bin:{os.environ.get('PATH', '/usr/bin:/bin')}"


def test_absent_uv_is_refused_naming_the_pinned_release_and_both_executables(
    tmp_path: Path,
) -> None:
    home = tmp_path / "home"
    path = _path_with_supported_python(home)

    completed = _run(_require_uv(home), home=home, path=path)

    assert completed.returncode == 1
    assert f"requires the pinned uv {UV_VERSION} at {home}/.local/bin/uv" in completed.stderr
    assert "nothing is there" in completed.stderr
    assert host_requirements.UV_LINUX_AMD64_ARCHIVE_URL in completed.stderr
    # The later ownership stage requires uvx beside uv, so the fix names both.
    assert "Extract uv and uvx" in completed.stderr
    assert "Traceback" not in completed.stderr


def test_stale_uv_is_refused_naming_found_and_expected(tmp_path: Path) -> None:
    home = tmp_path / "home"
    path = _path_with_supported_python(home)
    stale = _executable(
        home / ".local" / "bin" / "uv",
        "#!/bin/sh\necho 'uv 0.7.10 (x86_64-unknown-linux-gnu)'\n",
    )
    stale_sha256 = hashlib.sha256(stale.read_bytes()).hexdigest()

    completed = _run(_require_uv(home), home=home, path=path)

    assert completed.returncode == 1
    assert f"found uv 0.7.10 (x86_64-unknown-linux-gnu) (sha256 {stale_sha256})" in (
        completed.stderr
    )
    assert f"pinned uv {UV_VERSION} (executable sha256 {UV_LINUX_AMD64_EXECUTABLE_SHA256})" in (
        completed.stderr
    )
    assert host_requirements.UV_LINUX_AMD64_ARCHIVE_URL in completed.stderr


def test_pinned_uv_passes_the_source_check(tmp_path: Path) -> None:
    home = tmp_path / "home"
    path = _path_with_supported_python(home)
    pinned = _executable(home / ".local" / "bin" / "uv", "#!/bin/sh\necho 'uv pinned'\n")

    completed = _run(
        _require_uv(home, executable_sha256=hashlib.sha256(pinned.read_bytes()).hexdigest()),
        home=home,
        path=path,
    )

    assert completed.returncode == 0, completed.stderr
    assert completed.stderr == ""


def test_retained_state_path_checks_uv_before_its_pinned_copy() -> None:
    script = render_linux_user_bootstrap_script(frp_version="0.69.1")
    check = script.index('bootstrap_require_pinned_uv_source "$BOOTSTRAP_PINNED_UV_SOURCE"')
    source = script.index('BOOTSTRAP_PINNED_UV_SOURCE="$HOME/.local/bin/uv"')
    copy = script.index('"$BOOTSTRAP_PINNED_UV_SOURCE" "$BOOTSTRAP_PREPARING_ROOT"')

    assert source < check < copy
