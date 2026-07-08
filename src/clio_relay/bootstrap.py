"""Autonomous installation helpers for desktop and cluster targets."""

from __future__ import annotations

import os
import platform
import shlex
import shutil
import subprocess
import tarfile
import tempfile
import zipfile
from dataclasses import dataclass
from importlib import resources
from pathlib import Path
from urllib.request import urlretrieve

from clio_relay import __version__
from clio_relay.errors import ConfigurationError, RelayError

FRP_VERSION = "0.69.1"


@dataclass(frozen=True)
class BootstrapArchive:
    """Remote bootstrap archive and relay install source."""

    archive: Path
    install_spec: str


def install_local_frp(destination: Path) -> Path:
    """Install frpc/frps for the local platform into a user-writable directory."""
    system = platform.system().lower()
    machine = platform.machine().lower()
    destination.mkdir(parents=True, exist_ok=True)
    if system != "windows" or machine not in {"amd64", "x86_64"}:
        raise ConfigurationError(f"local frp installer does not support {system}/{machine}")
    frpc = destination / "frpc.exe"
    frps = destination / "frps.exe"
    if frpc.exists() and frps.exists():
        try:
            _assert_frp_pair(frpc, frps)
            return frpc
        except ConfigurationError:
            frpc.unlink(missing_ok=True)
            frps.unlink(missing_ok=True)
    errors: list[str] = []
    for installer in (
        _install_frp_from_scoop,
        _install_frp_from_source,
        _install_frp_from_release_archive,
    ):
        try:
            installer(destination, FRP_VERSION)
            _assert_frp_pair(frpc, frps)
            return frpc
        except ConfigurationError as exc:
            errors.append(f"{installer.__name__}: {exc}")
        except OSError as exc:
            errors.append(f"{installer.__name__}: {exc}")
    raise ConfigurationError("failed to install frp: " + "; ".join(errors))


def _install_frp_from_scoop(destination: Path, version: str) -> None:
    if shutil.which("scoop") is None:
        raise ConfigurationError("scoop is not available")
    result = _run_scoop(["list", "frp"], check=False)
    if result.returncode != 0 or version not in result.stdout:
        _run_scoop(["install", "frp"], check=True)
    frpc_source = _scoop_which("frpc")
    frps_source = _scoop_which("frps")
    shutil.copy2(frpc_source, destination / "frpc.exe")
    shutil.copy2(frps_source, destination / "frps.exe")


def _scoop_which(binary: str) -> Path:
    result = _run_scoop(["which", binary], check=False)
    if result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip()
        raise ConfigurationError(f"scoop cannot locate {binary}: {detail}")
    path = Path(result.stdout.strip().splitlines()[-1])
    if not path.exists():
        raise ConfigurationError(f"scoop reported missing path for {binary}: {path}")
    return path


def _run_scoop(args: list[str], *, check: bool) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        [
            "powershell",
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-Command",
            "scoop " + " ".join(shlex.quote(arg) for arg in args),
        ],
        text=True,
        capture_output=True,
        check=False,
    )
    if check and result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip()
        raise ConfigurationError(f"scoop command failed: {detail}")
    return result


def _install_frp_from_source(destination: Path, version: str) -> None:
    if shutil.which("git") is None or shutil.which("go") is None:
        raise ConfigurationError("git and go are required for source build")
    with tempfile.TemporaryDirectory(prefix="clio-relay-frp-") as temp_dir:
        source_dir = Path(temp_dir) / "frp"
        _run(
            [
                "git",
                "clone",
                "--depth",
                "1",
                "--branch",
                f"v{version}",
                "https://github.com/fatedier/frp.git",
                str(source_dir),
            ]
        )
        _run(
            [
                "go",
                "build",
                "-trimpath",
                "-tags",
                "frpc,noweb",
                "-o",
                "frpc.exe",
                "./cmd/frpc",
            ],
            cwd=source_dir,
        )
        _run(
            [
                "go",
                "build",
                "-trimpath",
                "-tags",
                "frps,noweb",
                "-o",
                "frps.exe",
                "./cmd/frps",
            ],
            cwd=source_dir,
        )
        shutil.copy2(source_dir / "frpc.exe", destination / "frpc.exe")
        shutil.copy2(source_dir / "frps.exe", destination / "frps.exe")


def _install_frp_from_release_archive(destination: Path, version: str) -> None:
    archive = destination.parent / f"frp_{version}_windows_amd64.zip"
    url = (
        "https://github.com/fatedier/frp/releases/download/"
        f"v{version}/frp_{version}_windows_amd64.zip"
    )
    urlretrieve(url, archive)
    with zipfile.ZipFile(archive) as zipped:
        zipped.extractall(destination.parent)
    extracted = destination.parent / f"frp_{version}_windows_amd64"
    shutil.copy2(extracted / "frpc.exe", destination / "frpc.exe")
    shutil.copy2(extracted / "frps.exe", destination / "frps.exe")


def bootstrap_cluster_over_ssh(
    *,
    bootstrap_profile: str,
    ssh_host: str,
    source_root: Path,
    agent_adapter: str = "exec",
    agent_npm_package: str | None = None,
    agent_npm_bin: str | None = None,
    agent_args: list[str] | None = None,
    frp_version: str = FRP_VERSION,
) -> list[str]:
    """Install relay dependencies and the current source tree on a cluster over SSH."""
    if bootstrap_profile != "linux-user":
        raise ConfigurationError(f"unsupported bootstrap profile: {bootstrap_profile}")
    if shutil.which("ssh") is None or shutil.which("scp") is None:
        raise ConfigurationError("ssh and scp are required for remote bootstrap")
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        archive = temp_path / "clio-relay-head.tar"
        script_path = temp_path / "clio-relay-bootstrap.sh"
        deployment = create_bootstrap_archive(source_root=source_root, archive=archive)
        _run(["scp", str(deployment.archive), f"{ssh_host}:/tmp/clio-relay-head.tar"])
        script_path.write_text(
            render_linux_user_bootstrap_script(
                frp_version=frp_version,
                agent_adapter=agent_adapter,
                agent_npm_package=agent_npm_package,
                agent_npm_bin=agent_npm_bin,
                agent_args=agent_args or [],
                relay_install_spec=deployment.install_spec,
            ),
            encoding="utf-8",
            newline="\n",
        )
        _run(["scp", str(script_path), f"{ssh_host}:/tmp/clio-relay-bootstrap.sh"])
    result = _run(["ssh", ssh_host, "bash", "/tmp/clio-relay-bootstrap.sh"])
    return result.stdout.splitlines()


def install_cluster_app_over_ssh(*, ssh_host: str, app_name: str) -> list[str]:
    """Install an application-specific runtime on a cluster over SSH."""
    if shutil.which("ssh") is None:
        raise ConfigurationError("ssh is required for remote app installation")
    script = render_cluster_app_install_script(app_name=app_name)
    result = subprocess.run(
        ["ssh", ssh_host, "bash", "-s"],
        input=script,
        text=True,
        capture_output=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )
    if result.returncode != 0:
        raise RelayError(
            f"cluster app installation failed on {ssh_host}: "
            f"{result.stderr.strip() or result.stdout.strip()}"
        )
    return result.stdout.splitlines()


def render_cluster_app_install_script(*, app_name: str) -> str:
    """Render an app-specific remote install script."""
    if app_name != "lammps":
        raise ConfigurationError(f"unsupported cluster app: {app_name}")
    return render_lammps_app_install_script()


def render_lammps_app_install_script() -> str:
    """Render the sudo-less LAMMPS installer used by explicit app setup."""
    return """set -euo pipefail
export PATH="$HOME/.local/bin:$PATH"
mkdir -p "$HOME/.local/bin" "$HOME/.local/src" "$HOME/.local/share/clio-relay/apps/lammps"

if [ ! -d "$HOME/spack/.git" ]; then
  git clone --depth 1 --branch releases/v1.1 https://github.com/spack/spack.git "$HOME/spack"
fi
. "$HOME/spack/share/spack/setup-env.sh"
spack compiler find || true
if ! spack find -p lammps >/dev/null 2>&1; then
  spack install lammps
fi
LAMMPS_PREFIX="$(spack location -i lammps)"
if [ -z "$LAMMPS_PREFIX" ] || [ ! -d "$LAMMPS_PREFIX" ]; then
  echo "failed to locate installed LAMMPS prefix through Spack" >&2
  exit 1
fi
LAMMPS_BIN=""
for candidate in \
  "$LAMMPS_PREFIX/bin/lmp" \
  "$LAMMPS_PREFIX/bin/lmp_mpi" \
  "$LAMMPS_PREFIX/bin/lammps"; do
  if [ -x "$candidate" ]; then
    LAMMPS_BIN="$candidate"
    break
  fi
done
if [ -z "$LAMMPS_BIN" ]; then
  echo "LAMMPS installed at $LAMMPS_PREFIX but no lmp/lmp_mpi/lammps binary was found" >&2
  exit 1
fi

cat > "$HOME/.local/share/clio-relay/apps/lammps/env.sh" <<__CLIO_RELAY_LAMMPS_ENV__
export SPACK_ROOT="$HOME/spack"
. "$HOME/spack/share/spack/setup-env.sh"
spack load lammps
export CLIO_RELAY_LAMMPS_BIN="$LAMMPS_BIN"
__CLIO_RELAY_LAMMPS_ENV__

cat > "$HOME/.local/bin/lmp" <<'__CLIO_RELAY_LAMMPS_ENTRYPOINT__'
#!/usr/bin/env bash
set -euo pipefail
. "$HOME/.local/share/clio-relay/apps/lammps/env.sh"
exec "$CLIO_RELAY_LAMMPS_BIN" "$@"
__CLIO_RELAY_LAMMPS_ENTRYPOINT__
chmod 0755 "$HOME/.local/bin/lmp"

echo "lammps_prefix=$LAMMPS_PREFIX"
echo "lammps_bin=$LAMMPS_BIN"
echo "lmp=$HOME/.local/bin/lmp"
"""


def render_linux_user_bootstrap_script(
    *,
    frp_version: str = FRP_VERSION,
    agent_adapter: str = "exec",
    agent_npm_package: str | None = None,
    agent_npm_bin: str | None = None,
    agent_args: list[str] | None = None,
    relay_install_spec: str = "$DEST",
) -> str:
    """Render the idempotent shell script used for the current Linux cluster bootstrap."""
    rendered_agent_adapter = shlex.quote(agent_adapter)
    rendered_agent_args = shlex.quote(" ".join(agent_args or []))
    rendered_agent_npm_package = shlex.quote(agent_npm_package or "")
    rendered_agent_npm_bin = shlex.quote(agent_npm_bin or "")
    rendered_relay_install_spec = (
        '"$DEST"' if relay_install_spec == "$DEST" else shlex.quote(relay_install_spec)
    )
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

AGENT_NPM_PACKAGE=${{CLIO_RELAY_AGENT_NPM_PACKAGE:-{rendered_agent_npm_package}}}
AGENT_NPM_BIN=${{CLIO_RELAY_AGENT_NPM_BIN:-{rendered_agent_npm_bin}}}
AGENT_BIN="${{CLIO_RELAY_AGENT_BIN:-}}"
if [ -z "$AGENT_BIN" ] && [ -n "$AGENT_NPM_BIN" ]; then
  AGENT_BIN="$HOME/.local/bin/$AGENT_NPM_BIN"
fi
if [ ! -x "$AGENT_BIN" ] && [ -n "$AGENT_NPM_PACKAGE" ] && command -v npm >/dev/null 2>&1; then
  npm install -g "$AGENT_NPM_PACKAGE"
fi

JARVIS_VENV="$HOME/.local/share/clio-relay/jarvis-venv"
uv venv --python 3.12 --seed --clear "$JARVIS_VENV"
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
uv pip install --refresh-package clio-relay {rendered_relay_install_spec}
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

cat > "$HOME/.local/bin/mpiexec" <<'__CLIO_RELAY_MPIEXEC_WRAPPER__'
#!/usr/bin/env bash
set -euo pipefail
if command -v mpiexec.real >/dev/null 2>&1; then
  exec mpiexec.real "$@"
fi
if [ "${{1:-}}" = "--version" ] || [ "${{1:-}}" = "-version" ]; then
  echo "mpich 4.0.0 clio-relay user-space wrapper"
  exit 0
fi
ranks=""
passthrough=()
while [ "$#" -gt 0 ]; do
  case "$1" in
    -n|-np)
      ranks="${{2:-}}"
      shift 2
      ;;
    -p|-f|--host|--hostfile|-host|-hostfile|--hosts|-hosts|--ppn|-ppn|-npernode)
      shift 2
      ;;
    -genv|--env)
      shift 3
      ;;
    -env)
      shift 2
      ;;
    -x)
      shift 2
      ;;
    --oversubscribe)
      shift
      ;;
    --)
      shift
      break
      ;;
    -*)
      passthrough+=("$1")
      shift
      ;;
    *=*)
      shift
      ;;
    [0-9]*)
      if [ -z "$ranks" ]; then
        ranks="$1"
      fi
      shift
      ;;
    *)
      break
      ;;
  esac
done
if [ "${{ranks:-1}}" = "1" ]; then
  exec "$@" "${{passthrough[@]}}"
fi
if command -v srun >/dev/null 2>&1; then
  exec srun -n "$ranks" "$@" "${{passthrough[@]}}"
fi
echo "mpiexec wrapper only supports single-rank runs without srun" >&2
exit 127
exec "$@"
__CLIO_RELAY_MPIEXEC_WRAPPER__
chmod 0755 "$HOME/.local/bin/mpiexec"

CLIO_RELAY_CORE_DIR="$HOME/.local/share/clio-relay/core" \
CLIO_RELAY_SPOOL_DIR="$HOME/.local/share/clio-relay/spool" \
CLIO_RELAY_JARVIS_BIN="$HOME/.local/bin/jarvis" \
CLIO_RELAY_FRPC_BIN="$HOME/.local/bin/frpc" \
CLIO_RELAY_AGENT_BIN="${{AGENT_BIN:-agent}}" \
CLIO_RELAY_AGENT_ADAPTER={rendered_agent_adapter} \
CLIO_RELAY_AGENT_ARGS={rendered_agent_args} \
clio-relay init

echo "frpc=$("$HOME/.local/bin/frpc" --version)"
echo "frps=$("$HOME/.local/bin/frps" --version)"
if [ -x "$AGENT_BIN" ]; then
  echo "agent=$("$AGENT_BIN" --version)"
fi
echo "jarvis=$("$HOME/.local/bin/jarvis" --help | head -n 1)"
echo "relay=$(clio-relay --help | head -n 1)"
"""
    return script.replace("\r\n", "\n")


def create_bootstrap_archive(*, source_root: Path, archive: Path) -> BootstrapArchive:
    """Create the archive used by remote bootstrap.

    A clean git checkout deploys that exact committed tree. Installed-package
    runs deploy packaged JARVIS assets and install clio-relay from PyPI by
    version, so bootstrap does not require a checkout.
    """
    if (source_root / ".git").exists():
        assert_clean_git_checkout(source_root)
        _run(["git", "archive", "--format=tar", "-o", str(archive), "HEAD"], cwd=source_root)
        return BootstrapArchive(archive=archive, install_spec="$DEST")
    _write_packaged_bootstrap_archive(archive)
    return BootstrapArchive(archive=archive, install_spec=f"clio-relay=={__version__}")


def _write_packaged_bootstrap_archive(archive: Path) -> None:
    assets = resources.files("clio_relay").joinpath("assets", "jarvis-packages")
    source_assets = Path(__file__).resolve().parents[2] / "jarvis-packages"
    with tarfile.open(archive, "w") as tar:
        if assets.is_dir():
            with resources.as_file(assets) as asset_path:
                _add_jarvis_assets_to_archive(tar=tar, asset_path=asset_path)
            return
        if source_assets.is_dir():
            _add_jarvis_assets_to_archive(tar=tar, asset_path=source_assets)
            return
    raise ConfigurationError("installed clio-relay package does not include jarvis package assets")


def _add_jarvis_assets_to_archive(*, tar: tarfile.TarFile, asset_path: Path) -> None:
    for item in asset_path.rglob("*"):
        relative_parts = item.relative_to(asset_path).parts
        if "__pycache__" in relative_parts or item.name.endswith(".pyc"):
            continue
        tar.add(
            item,
            arcname=str(Path("jarvis-packages", *relative_parts)),
            recursive=False,
        )


def assert_clean_git_checkout(source_root: Path) -> None:
    """Raise if source_root has uncommitted changes that git archive would omit."""
    result = subprocess.run(
        ["git", "status", "--porcelain"],
        cwd=source_root,
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip()
        raise RelayError(f"failed to inspect git checkout before bootstrap: {detail}")
    if result.stdout.strip():
        raise ConfigurationError(
            "remote bootstrap deploys git HEAD; commit or stash local changes before bootstrap"
        )


def _assert_executable(path: Path) -> None:
    try:
        subprocess.run([str(path), "--version"], check=True, capture_output=True, text=True)
    except (OSError, subprocess.CalledProcessError) as exc:
        raise ConfigurationError(f"installed executable cannot run: {path}: {exc}") from exc


def _assert_frp_pair(frpc: Path, frps: Path) -> None:
    _assert_executable(frpc)
    _assert_executable(frps)


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
        encoding="utf-8",
        errors="replace",
        capture_output=True,
        check=False,
        env=env,
    )
    if result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip()
        raise RelayError(f"command failed ({' '.join(command)}): {detail}")
    return result
