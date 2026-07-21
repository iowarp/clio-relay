from __future__ import annotations

import os
import stat
import subprocess
import sys
from pathlib import Path

from clio_relay.bootstrap import render_linux_user_bootstrap_script


def _embedded_python(script: str, marker: str) -> str:
    """Extract one exact Python heredoc from the rendered Linux bootstrap."""
    opening = f"<<'{marker}'\n"
    start = script.index(opening) + len(opening)
    end = script.index(f"\n{marker}\n", start)
    return script[start:end]


def _lock_acquisition_source() -> str:
    """Return the exact initial lock acquisition program shipped to a cluster."""
    return _embedded_python(
        render_linux_user_bootstrap_script(),
        "__CLIO_RELAY_BOOTSTRAP_LOCK_AND_REEXEC__",
    )


def _lock_verification_source() -> str:
    """Return the exact inherited-lock verifier shipped to a cluster."""
    return _embedded_python(
        render_linux_user_bootstrap_script(),
        "__CLIO_RELAY_BOOTSTRAP_LOCK_VERIFY__",
    )


def _lock_test_target(path: Path, *, verification_source: str) -> None:
    """Write a target that proves the inherited descriptor owns the live lock."""
    path.write_text(
        """set -euo pipefail
python3 - <<'__CLIO_RELAY_EXACT_LOCK_VERIFY__'
"""
        + verification_source
        + """
__CLIO_RELAY_EXACT_LOCK_VERIFY__
python3 - <<'__CLIO_RELAY_LOCK_TEST__'
import fcntl
import os
import stat
from pathlib import Path

directory = Path.home() / ".local/share/clio-relay"
lock_path = directory / "bootstrap.lock"
assert os.environ["CLIO_RELAY_BOOTSTRAP_LOCK_FD"] == "9"
assert stat.S_IMODE(directory.stat().st_mode) == 0o700
assert stat.S_IMODE(lock_path.stat().st_mode) == 0o600
assert os.fstat(9).st_ino == lock_path.stat().st_ino
probe = os.open(lock_path, os.O_RDWR)
try:
    try:
        fcntl.flock(probe, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        pass
    else:
        raise AssertionError("a second open file description acquired the bootstrap lock")
finally:
    os.close(probe)
print("bootstrap_lock_upgrade=ready")
__CLIO_RELAY_LOCK_TEST__
""",
        encoding="utf-8",
    )


def test_embedded_bootstrap_lock_repairs_owned_legacy_directory_and_holds_lock(
    tmp_path: Path,
) -> None:
    """An owner-controlled legacy 0755 directory is safely upgraded before locking."""
    source = _lock_acquisition_source()
    assert "dir_fd=directory_descriptor" in source
    assert source.index("opened_directory.st_uid != os.getuid()") < source.index(
        "os.fchmod(directory_descriptor, 0o700)"
    )
    assert source.index("opened.st_uid != os.getuid()") < source.index(
        "os.fchmod(descriptor, 0o600)"
    )
    if os.name != "posix":
        return

    home = tmp_path / "home"
    directory = home / ".local/share/clio-relay"
    directory.mkdir(parents=True)
    directory.chmod(0o755)
    lock_path = directory / "bootstrap.lock"
    lock_path.write_bytes(b"")
    lock_path.chmod(0o644)
    target = tmp_path / "lock-target.sh"
    _lock_test_target(target, verification_source=_lock_verification_source())
    environment = {**os.environ, "HOME": str(home)}
    environment.pop("CLIO_RELAY_BOOTSTRAP_LOCK_FD", None)

    result = subprocess.run(
        [sys.executable, "-c", source, str(target)],
        capture_output=True,
        check=False,
        env=environment,
        text=True,
        timeout=15,
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "bootstrap_lock_upgrade=ready"
    assert stat.S_IMODE(directory.stat().st_mode) == 0o700
    assert stat.S_IMODE(lock_path.stat().st_mode) == 0o600


def test_embedded_bootstrap_lock_does_not_follow_or_chmod_directory_symlink(
    tmp_path: Path,
) -> None:
    """A redirected state path remains rejected without modifying its target."""
    source = _lock_acquisition_source()
    assert "getattr(os, flag_name, 0)" in source
    assert 'flag_name in ("O_CLOEXEC", "O_DIRECTORY", "O_NOFOLLOW")' in source
    if os.name != "posix":
        return

    home = tmp_path / "home"
    share = home / ".local/share"
    share.mkdir(parents=True)
    redirected = tmp_path / "redirected"
    redirected.mkdir()
    redirected.chmod(0o755)
    (share / "clio-relay").symlink_to(redirected, target_is_directory=True)
    target = tmp_path / "must-not-run.sh"
    target.write_text("exit 99\n", encoding="utf-8")
    environment = {**os.environ, "HOME": str(home)}
    environment.pop("CLIO_RELAY_BOOTSTRAP_LOCK_FD", None)

    result = subprocess.run(
        [sys.executable, "-c", source, str(target)],
        capture_output=True,
        check=False,
        env=environment,
        text=True,
        timeout=15,
    )

    assert result.returncode != 0
    assert "bootstrap lock directory must be a real directory" in result.stderr
    assert stat.S_IMODE(redirected.stat().st_mode) == 0o755
    assert not (redirected / "bootstrap.lock").exists()
