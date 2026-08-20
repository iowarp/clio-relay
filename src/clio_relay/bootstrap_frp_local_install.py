"""Local (Windows) frp install for the Linux/Windows cluster bootstrap CLI.

Split from bootstrap.py (clio-relay#255). `install_local_frp` installs frpc/frps for the
operator's own Windows machine (used by ``clio-relay install-frp``, distinct from the
remote Linux cluster's own frp install embedded in the rendered bootstrap script); its
four helpers (directory cleanup, release-archive fetch/verify, and executable/digest
assertion) are private to this concern. None of the six names are monkeypatched;
`_assert_executable`'s own `_run` call is the one exception, rewritten to a qualified,
call-time `bootstrap._run(...)` lookup so `monkeypatch.setattr(bootstrap, "_run", ...)`
still reaches it.
"""

from __future__ import annotations

import hashlib
import platform
import shutil
import tempfile
import zipfile
from pathlib import Path
from urllib.request import urlretrieve

from clio_relay.bootstrap_constants import (
    FRP_VERSION,
    FRP_WINDOWS_AMD64_SHA256,
    FRPC_WINDOWS_AMD64_SHA256,
    FRPS_WINDOWS_AMD64_SHA256,
)
from clio_relay.errors import ConfigurationError, RelayError


def install_local_frp(destination: Path) -> Path:
    """Install frpc/frps for the local platform into a user-writable directory."""
    import clio_relay.bootstrap as bootstrap

    system = platform.system().lower()
    machine = platform.machine().lower()
    if system != "windows" or machine not in {"amd64", "x86_64"}:
        raise ConfigurationError(f"local frp installer does not support {system}/{machine}")
    destination.mkdir(parents=True, exist_ok=True)
    frpc = destination / "frpc.exe"
    frps = destination / "frps.exe"
    if frpc.exists() and frps.exists():
        try:
            bootstrap._assert_frp_pair(frpc, frps)
            return frpc
        except ConfigurationError:
            pass
    cleanup_errors = _remove_local_frp_pair(frpc, frps)
    if cleanup_errors:
        raise ConfigurationError(
            "could not remove an unverified existing frp installation: " + "; ".join(cleanup_errors)
        )
    try:
        with tempfile.TemporaryDirectory(
            prefix=".clio-relay-frp-",
            dir=destination.parent,
        ) as temporary_directory:
            staging = Path(temporary_directory) / "bin"
            staging.mkdir()
            bootstrap._install_frp_from_release_archive(staging, FRP_VERSION)
            staged_frpc = staging / "frpc.exe"
            staged_frps = staging / "frps.exe"
            bootstrap._assert_frp_pair(staged_frpc, staged_frps)
            shutil.copy2(staged_frpc, frpc)
            shutil.copy2(staged_frps, frps)
            bootstrap._assert_frp_pair(frpc, frps)
        bootstrap._assert_frp_pair(frpc, frps)
        return frpc
    except (ConfigurationError, OSError) as exc:
        cleanup_errors = _remove_local_frp_pair(frpc, frps)
        cleanup_detail = (
            ""
            if not cleanup_errors
            else "; unverified destination cleanup failed: " + "; ".join(cleanup_errors)
        )
        raise ConfigurationError(
            f"failed to install verified frp release: {exc}{cleanup_detail}"
        ) from exc


def _remove_local_frp_pair(frpc: Path, frps: Path) -> list[str]:
    """Remove both local frp executables and return any cleanup errors."""
    errors: list[str] = []
    for path in (frpc, frps):
        try:
            path.unlink(missing_ok=True)
        except OSError as exc:
            errors.append(f"{path}: {exc}")
    return errors


def _install_frp_from_release_archive(destination: Path, version: str) -> None:
    if version != FRP_VERSION:
        raise ConfigurationError(f"no pinned Windows checksum is registered for frp {version}")
    archive = destination.parent / f"frp_{version}_windows_amd64.zip"
    url = (
        "https://github.com/fatedier/frp/releases/download/"
        f"v{version}/frp_{version}_windows_amd64.zip"
    )
    urlretrieve(url, archive)
    observed = hashlib.sha256(archive.read_bytes()).hexdigest()
    if observed != FRP_WINDOWS_AMD64_SHA256:
        raise ConfigurationError(
            f"frp archive SHA-256 mismatch: {observed} != {FRP_WINDOWS_AMD64_SHA256}"
        )
    with zipfile.ZipFile(archive) as zipped:
        zipped.extractall(destination.parent)
    extracted = destination.parent / f"frp_{version}_windows_amd64"
    shutil.copy2(extracted / "frpc.exe", destination / "frpc.exe")
    shutil.copy2(extracted / "frps.exe", destination / "frps.exe")


def _assert_executable(path: Path) -> None:
    import clio_relay.bootstrap as bootstrap

    try:
        bootstrap._run(
            [str(path), "--version"],
            timeout_seconds=10,
            stdout_maximum_bytes=4096,
            stderr_maximum_bytes=4096,
        )
    except (OSError, RelayError) as exc:
        raise ConfigurationError(f"installed executable cannot run: {path}: {exc}") from exc


def _assert_frp_pair(frpc: Path, frps: Path) -> None:
    _assert_sha256(frpc, FRPC_WINDOWS_AMD64_SHA256)
    _assert_sha256(frps, FRPS_WINDOWS_AMD64_SHA256)
    _assert_executable(frpc)
    _assert_executable(frps)


def _assert_sha256(path: Path, expected: str) -> None:
    try:
        observed = hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError as exc:
        raise ConfigurationError(f"installed executable cannot be hashed: {path}: {exc}") from exc
    if observed != expected:
        raise ConfigurationError(f"installed executable SHA-256 mismatch: {path}: {observed}")
