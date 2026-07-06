"""Autonomous installation helpers for desktop and Ares targets."""

from __future__ import annotations

import os
import platform
import shutil
import subprocess
import tempfile
import zipfile
from pathlib import Path
from urllib.request import urlretrieve

from clio_relay.errors import ConfigurationError, RelayError

FRP_VERSION = "0.69.1"


def install_local_frp(destination: Path) -> Path:
    """Install frpc/frps for the local platform into a user-writable directory."""
    system = platform.system().lower()
    machine = platform.machine().lower()
    destination.mkdir(parents=True, exist_ok=True)
    if system != "windows" or machine not in {"amd64", "x86_64"}:
        raise ConfigurationError(f"local frp installer does not support {system}/{machine}")
    frpc = destination / "frpc.exe"
    if frpc.exists():
        _assert_executable(frpc)
        return frpc
    archive = destination.parent / f"frp_{FRP_VERSION}_windows_amd64.zip"
    url = (
        "https://github.com/fatedier/frp/releases/download/"
        f"v{FRP_VERSION}/frp_{FRP_VERSION}_windows_amd64.zip"
    )
    urlretrieve(url, archive)
    with zipfile.ZipFile(archive) as zipped:
        zipped.extractall(destination.parent)
    extracted = destination.parent / f"frp_{FRP_VERSION}_windows_amd64"
    shutil.copy2(extracted / "frpc.exe", destination / "frpc.exe")
    shutil.copy2(extracted / "frps.exe", destination / "frps.exe")
    _assert_executable(frpc)
    return frpc


def bootstrap_ares_over_ssh(
    *,
    ssh_host: str,
    source_root: Path,
    frp_version: str = FRP_VERSION,
) -> list[str]:
    """Install relay dependencies and the current source tree on Ares over SSH."""
    if shutil.which("ssh") is None or shutil.which("scp") is None or shutil.which("git") is None:
        raise ConfigurationError("ssh, scp, and git are required for remote Ares bootstrap")
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        archive = temp_path / "clio-relay-head.tar"
        script_path = temp_path / "clio-relay-bootstrap.sh"
        _run(["git", "archive", "--format=tar", "-o", str(archive), "HEAD"], cwd=source_root)
        _run(["scp", str(archive), f"{ssh_host}:/tmp/clio-relay-head.tar"])
        script_path.write_text(
            render_ares_bootstrap_script(frp_version=frp_version),
            encoding="utf-8",
            newline="\n",
        )
        _run(["scp", str(script_path), f"{ssh_host}:/tmp/clio-relay-bootstrap.sh"])
    result = _run(["ssh", ssh_host, "bash", "/tmp/clio-relay-bootstrap.sh"])
    return result.stdout.splitlines()


def render_ares_bootstrap_script(*, frp_version: str = FRP_VERSION) -> str:
    """Render the idempotent shell script used for Ares bootstrap."""
    script = f"""set -euo pipefail
export PATH="$HOME/.local/bin:$PATH"
mkdir -p "$HOME/.local/bin" "$HOME/.local/src" "$HOME/.local/share/clio-relay"

cd "$HOME/.local/src"
FRP_VERSION="{frp_version}"
ARCHIVE="frp_${{FRP_VERSION}}_linux_amd64.tar.gz"
if [ ! -x "$HOME/.local/bin/frpc" ] || [ ! -x "$HOME/.local/bin/frps" ]; then
  curl -L --fail --retry 3 -o "$ARCHIVE" \
    "https://github.com/fatedier/frp/releases/download/v${{FRP_VERSION}}/${{ARCHIVE}}"
  tar -xzf "$ARCHIVE"
  install -m 0755 "frp_${{FRP_VERSION}}_linux_amd64/frpc" "$HOME/.local/bin/frpc"
  install -m 0755 "frp_${{FRP_VERSION}}_linux_amd64/frps" "$HOME/.local/bin/frps"
fi

if [ ! -x "$HOME/.local/bin/uv" ]; then
  curl -LsSf https://astral.sh/uv/install.sh | sh
fi
uv python install 3.12

if [ ! -x "$HOME/.local/bin/codex" ] && command -v npm >/dev/null 2>&1; then
  npm install -g @openai/codex
fi

JARVIS_VENV="$HOME/.local/share/clio-relay/jarvis-venv"
python3 -m venv "$JARVIS_VENV"
. "$JARVIS_VENV/bin/activate"
python -m pip install --upgrade pip setuptools wheel
if [ ! -d "$HOME/.local/src/jarvis-util/.git" ]; then
  git clone https://github.com/grc-iit/jarvis-util.git "$HOME/.local/src/jarvis-util"
fi
git -C "$HOME/.local/src/jarvis-util" pull --ff-only
python -m pip install -r "$HOME/.local/src/jarvis-util/requirements.txt"
python -m pip install -e "$HOME/.local/src/jarvis-util"
if [ ! -d "$HOME/.local/src/jarvis-cd/.git" ]; then
  git clone https://github.com/grc-iit/jarvis-cd.git "$HOME/.local/src/jarvis-cd"
fi
git -C "$HOME/.local/src/jarvis-cd" pull --ff-only
python -m pip install -r "$HOME/.local/src/jarvis-cd/requirements.txt"
python -m pip install -e "$HOME/.local/src/jarvis-cd"
ln -sf "$JARVIS_VENV/bin/jarvis" "$HOME/.local/bin/jarvis"
deactivate

DEST="$HOME/.local/src/clio-relay"
rm -rf "$DEST"
mkdir -p "$DEST"
tar -xf /tmp/clio-relay-head.tar -C "$DEST"
uv venv --python 3.12 --clear "$HOME/.local/share/clio-relay/relay-venv312"
. "$HOME/.local/share/clio-relay/relay-venv312/bin/activate"
uv pip install "$DEST"
ln -sf "$HOME/.local/share/clio-relay/relay-venv312/bin/clio-relay" "$HOME/.local/bin/clio-relay"
deactivate

mkdir -p \
  "$HOME/.local/share/clio-relay/jarvis-config" \
  "$HOME/.local/share/clio-relay/jarvis-private" \
  "$HOME/.local/share/clio-relay/jarvis-shared"
jarvis init \
  "$HOME/.local/share/clio-relay/jarvis-config" \
  "$HOME/.local/share/clio-relay/jarvis-private" \
  "$HOME/.local/share/clio-relay/jarvis-shared" || true
jarvis repo add "$DEST/jarvis-packages/clio_relay" --force true

CLIO_RELAY_CORE_DIR="$HOME/.local/share/clio-relay/core" \
CLIO_RELAY_SPOOL_DIR="$HOME/.local/share/clio-relay/spool" \
CLIO_RELAY_JARVIS_BIN="$HOME/.local/bin/jarvis" \
CLIO_RELAY_FRPC_BIN="$HOME/.local/bin/frpc" \
CLIO_RELAY_CODEX_BIN="$HOME/.local/bin/codex" \
clio-relay init

echo "frpc=$("$HOME/.local/bin/frpc" --version)"
echo "frps=$("$HOME/.local/bin/frps" --version)"
if [ -x "$HOME/.local/bin/codex" ]; then
  echo "codex=$("$HOME/.local/bin/codex" --version)"
fi
echo "jarvis=$("$HOME/.local/bin/jarvis" --help | head -n 1)"
echo "relay=$(clio-relay --help | head -n 1)"
"""
    return script.replace("\r\n", "\n")


def _assert_executable(path: Path) -> None:
    try:
        subprocess.run([str(path), "--version"], check=True, capture_output=True, text=True)
    except (OSError, subprocess.CalledProcessError) as exc:
        raise ConfigurationError(f"installed executable cannot run: {path}: {exc}") from exc


def _run(
    command: list[str],
    *,
    cwd: Path | None = None,
) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    result = subprocess.run(
        command,
        cwd=cwd,
        text=True,
        capture_output=True,
        check=False,
        env=env,
    )
    if result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip()
        raise RelayError(f"command failed ({' '.join(command)}): {detail}")
    return result
