"""Autonomous installation helpers for desktop and cluster targets."""

from __future__ import annotations

import hashlib
import json
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
from typing import cast
from urllib.request import urlretrieve
from uuid import uuid4

from clio_relay import __version__
from clio_relay.errors import ConfigurationError, RelayError
from clio_relay.jarvis_mcp import CLIO_KIT_JARVIS_MCP_VERSION

FRP_VERSION = "0.69.1"
FRP_WINDOWS_AMD64_SHA256 = "829ac915f8655d4d4e021b8db61b46c3445205ed80d32b04cda7fa89d87c46e0"
FRP_LINUX_AMD64_SHA256 = "7be257b72dbbc60bcb3e0e25a5afd1dfac7b63f897084864d3c956dd3d5674e1"
FRPC_LINUX_AMD64_SHA256 = "142f447f43fef286acc8da8a6852dda80631db631d604b2e63634b2db4d6848c"
FRPS_LINUX_AMD64_SHA256 = "68d2908bb73fe7a03c29d9227d2acc2104bff3fea6b1cece0b8388c1a0660442"
FRPC_WINDOWS_AMD64_SHA256 = "1d1c4f988b1808bb458a4ba38f00359052d14636023a504520e0afed127d636d"
FRPS_WINDOWS_AMD64_SHA256 = "bd463ef89370abc6973c86258256fa65776baa5f515ef91ebeabd6070b92e229"
UV_VERSION = "0.11.28"
UV_LINUX_AMD64_SHA256 = "e490a6464492183c5d4534a5527fb4440f7f2bb2f228162ad7e4afe076dc0224"
JARVIS_UTIL_COMMIT = "c91bfdc9bba802e4b03bfb1babe614ffa3e09644"
JARVIS_CD_VERSION = "2.0.0"
JARVIS_CD_WHEEL_FILENAME = f"jarvis_cd-{JARVIS_CD_VERSION}-py3-none-any.whl"
JARVIS_CD_WHEEL_URL = (
    "https://github.com/grc-iit/jarvis-cd/releases/download/"
    f"v{JARVIS_CD_VERSION}/{JARVIS_CD_WHEEL_FILENAME}"
)
JARVIS_CD_WHEEL_SHA256 = "PENDING_JARVIS_CD_2_0_0_RELEASE_WHEEL_SHA256"


@dataclass(frozen=True)
class BootstrapArchive:
    """Remote bootstrap archive and relay install source."""

    archive: Path
    install_spec: str


def install_local_frp(destination: Path) -> Path:
    """Install frpc/frps for the local platform into a user-writable directory."""
    system = platform.system().lower()
    machine = platform.machine().lower()
    if system != "windows" or machine not in {"amd64", "x86_64"}:
        raise ConfigurationError(f"local frp installer does not support {system}/{machine}")
    destination.mkdir(parents=True, exist_ok=True)
    frpc = destination / "frpc.exe"
    frps = destination / "frps.exe"
    if frpc.exists() and frps.exists():
        try:
            _assert_frp_pair(frpc, frps)
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
            _install_frp_from_release_archive(staging, FRP_VERSION)
            staged_frpc = staging / "frpc.exe"
            staged_frps = staging / "frps.exe"
            _assert_frp_pair(staged_frpc, staged_frps)
            shutil.copy2(staged_frpc, frpc)
            shutil.copy2(staged_frps, frps)
            _assert_frp_pair(frpc, frps)
        _assert_frp_pair(frpc, frps)
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


def bootstrap_cluster_over_ssh(
    *,
    bootstrap_profile: str,
    ssh_host: str,
    source_root: Path,
    relay_wheel: Path | None = None,
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
        invocation_id = f"bootstrap_{uuid4().hex}"
        temp_path = Path(temp_dir)
        archive = temp_path / "clio-relay-head.tar"
        script_path = temp_path / "clio-relay-bootstrap.sh"
        deployment = create_bootstrap_archive(
            source_root=source_root,
            archive=archive,
            relay_wheel=relay_wheel,
        )
        _run(["scp", str(deployment.archive), f"{ssh_host}:/tmp/clio-relay-head.tar"])
        script_path.write_text(
            render_linux_user_bootstrap_script(
                frp_version=frp_version,
                agent_adapter=agent_adapter,
                agent_npm_package=agent_npm_package,
                agent_npm_bin=agent_npm_bin,
                agent_args=agent_args or [],
                relay_install_spec=deployment.install_spec,
                invocation_id=invocation_id,
            ),
            encoding="utf-8",
            newline="\n",
        )
        _run(["scp", str(script_path), f"{ssh_host}:/tmp/clio-relay-bootstrap.sh"])
    result = _run(["ssh", ssh_host, "bash", "/tmp/clio-relay-bootstrap.sh"])
    receipt_result = _run(
        ["ssh", ssh_host, "cat", "$HOME/.local/share/clio-relay/bootstrap-receipt.json"]
    )
    try:
        raw_receipt = cast(object, json.loads(receipt_result.stdout))
    except json.JSONDecodeError as exc:
        raise RelayError(f"bootstrap receipt was not valid JSON: {exc}") from exc
    if not isinstance(raw_receipt, dict):
        raise RelayError("bootstrap receipt was not a JSON object")
    receipt = cast(dict[str, object], raw_receipt)
    if receipt.get("invocation_id") != invocation_id:
        raise RelayError("bootstrap receipt does not match the completed invocation")
    install_receipt_sha256 = receipt.get("install_receipt_sha256")
    receipt_contract = {
        "schema_version": receipt.get("schema_version") == "clio-relay.bootstrap-receipt.v1",
        "bootstrap_profile": receipt.get("bootstrap_profile") == bootstrap_profile,
        "relay_install_spec": receipt.get("relay_install_spec") == deployment.install_spec,
        "install_receipt_sha256": isinstance(install_receipt_sha256, str)
        and len(install_receipt_sha256) == 64
        and all(character in "0123456789abcdef" for character in install_receipt_sha256),
        "completed_at": isinstance(receipt.get("completed_at"), str)
        and bool(receipt.get("completed_at")),
    }
    failed_contract = sorted(name for name, passed in receipt_contract.items() if not passed)
    if failed_contract:
        raise RelayError(f"bootstrap receipt contract failed: {failed_contract}")
    return [
        *result.stdout.splitlines(),
        "bootstrap_receipt_json=" + json.dumps(receipt, sort_keys=True),
    ]


def package_source_root() -> Path:
    """Return the project root for editable installs, or the package root for wheels."""
    return Path(__file__).resolve().parents[2]


def render_linux_user_bootstrap_script(
    *,
    frp_version: str = FRP_VERSION,
    agent_adapter: str = "exec",
    agent_npm_package: str | None = None,
    agent_npm_bin: str | None = None,
    agent_args: list[str] | None = None,
    relay_install_spec: str = "$DEST",
    jarvis_mcp_install_spec: str | None = None,
    invocation_id: str = "manual",
) -> str:
    """Render the idempotent shell script used for the current Linux cluster bootstrap."""
    rendered_agent_adapter = shlex.quote(agent_adapter)
    rendered_agent_args = shlex.quote(" ".join(agent_args or []))
    rendered_agent_npm_package = shlex.quote(agent_npm_package or "")
    rendered_agent_npm_bin = shlex.quote(agent_npm_bin or "")
    rendered_relay_install_spec = _render_relay_install_spec(relay_install_spec)
    if frp_version != FRP_VERSION:
        raise ConfigurationError(f"no pinned Linux checksum is registered for frp {frp_version}")
    rendered_jarvis_mcp_install_spec = shlex.quote(
        jarvis_mcp_install_spec
        or os.environ.get(
            "CLIO_RELAY_JARVIS_MCP_INSTALL_SPEC",
            f"clio-kit=={CLIO_KIT_JARVIS_MCP_VERSION}",
        )
    )
    script = f"""set -euo pipefail
export PATH="$HOME/.local/bin:$PATH"
mkdir -p "$HOME/.local/bin" "$HOME/.local/src" "$HOME/.local/share/clio-relay"

cd "$HOME/.local/src"
FRP_VERSION="{frp_version}"
FRP_SHA256="{FRP_LINUX_AMD64_SHA256}"
FRPC_SHA256="{FRPC_LINUX_AMD64_SHA256}"
FRPS_SHA256="{FRPS_LINUX_AMD64_SHA256}"
ARCHIVE="frp_${{FRP_VERSION}}_linux_amd64.tar.gz"
if [ ! -x "$HOME/.local/bin/frpc" ] \
  || [ ! -x "$HOME/.local/bin/frps" ] \
  || ! echo "$FRPC_SHA256 *$HOME/.local/bin/frpc" | sha256sum --check --status - \
  || ! echo "$FRPS_SHA256 *$HOME/.local/bin/frps" | sha256sum --check --status -; then
  curl -L --fail --retry 3 -o "$ARCHIVE" \
    "https://github.com/fatedier/frp/releases/download/v${{FRP_VERSION}}/${{ARCHIVE}}"
  echo "$FRP_SHA256 *$ARCHIVE" | sha256sum --check --strict -
  tar -xzf "$ARCHIVE"
  install -m 0755 "frp_${{FRP_VERSION}}_linux_amd64/frpc" "$HOME/.local/bin/frpc"
  install -m 0755 "frp_${{FRP_VERSION}}_linux_amd64/frps" "$HOME/.local/bin/frps"
  echo "$FRPC_SHA256 *$HOME/.local/bin/frpc" | sha256sum --check --strict -
  echo "$FRPS_SHA256 *$HOME/.local/bin/frps" | sha256sum --check --strict -
fi

UV_VERSION="{UV_VERSION}"
UV_SHA256="{UV_LINUX_AMD64_SHA256}"
UV_ARCHIVE="uv-x86_64-unknown-linux-gnu.tar.gz"
if [ ! -x "$HOME/.local/bin/uv" ] \
  || [ "$("$HOME/.local/bin/uv" --version | awk '{{print $1 " " $2}}')" != "uv $UV_VERSION" ]; then
  curl -L --fail --retry 3 -o "$UV_ARCHIVE" \
    "https://github.com/astral-sh/uv/releases/download/$UV_VERSION/$UV_ARCHIVE"
  echo "$UV_SHA256 *$UV_ARCHIVE" | sha256sum --check --strict -
  tar -xzf "$UV_ARCHIVE"
  install -m 0755 "uv-x86_64-unknown-linux-gnu/uv" "$HOME/.local/bin/uv"
  install -m 0755 "uv-x86_64-unknown-linux-gnu/uvx" "$HOME/.local/bin/uvx"
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
JARVIS_UTIL_COMMIT="{JARVIS_UTIL_COMMIT}"
if [ ! -d "$HOME/.local/src/jarvis-util/.git" ]; then
  git clone --no-checkout https://github.com/grc-iit/jarvis-util.git \
    "$HOME/.local/src/jarvis-util"
fi
if [ -n "$(
  git -C "$HOME/.local/src/jarvis-util" status --porcelain=v1 --untracked-files=all
)" ]; then
  echo "refusing to replace modified jarvis-util checkout" >&2
  exit 1
fi
git -C "$HOME/.local/src/jarvis-util" fetch --depth 1 origin "$JARVIS_UTIL_COMMIT"
git -C "$HOME/.local/src/jarvis-util" checkout --detach "$JARVIS_UTIL_COMMIT"
test "$(git -C "$HOME/.local/src/jarvis-util" rev-parse HEAD)" = "$JARVIS_UTIL_COMMIT"
python -m pip install -r "$HOME/.local/src/jarvis-util/requirements.txt"
python -m pip install "$HOME/.local/src/jarvis-util"
JARVIS_CD_VERSION="{JARVIS_CD_VERSION}"
JARVIS_CD_WHEEL_URL="{JARVIS_CD_WHEEL_URL}"
JARVIS_CD_WHEEL_SHA256="{JARVIS_CD_WHEEL_SHA256}"
JARVIS_CD_WHEEL_DIR="$HOME/.local/share/clio-relay/component-wheels/jarvis-cd"
JARVIS_CD_WHEEL="$JARVIS_CD_WHEEL_DIR/{JARVIS_CD_WHEEL_FILENAME}"
rm -rf "$JARVIS_CD_WHEEL_DIR"
mkdir -p "$JARVIS_CD_WHEEL_DIR"
JARVIS_CD_STAGING="$(mktemp "${{JARVIS_CD_WHEEL}}.XXXXXX")"
curl -L --fail --retry 3 -o "$JARVIS_CD_STAGING" "$JARVIS_CD_WHEEL_URL"
echo "$JARVIS_CD_WHEEL_SHA256 *$JARVIS_CD_STAGING" | sha256sum --check --strict -
mv "$JARVIS_CD_STAGING" "$JARVIS_CD_WHEEL"
python -m pip install "$JARVIS_CD_WHEEL"
ln -sf "$JARVIS_VENV/bin/jarvis" "$HOME/.local/bin/jarvis"
JARVIS_MCP_INSTALL_SPEC={rendered_jarvis_mcp_install_spec}
JARVIS_MCP_INSTALL_TARGET="$JARVIS_MCP_INSTALL_SPEC"
JARVIS_MCP_ARTIFACT_PATH=""
JARVIS_MCP_REQUESTED_SOURCE="checkout"
JARVIS_MCP_VERSION=""
case "$JARVIS_MCP_INSTALL_SPEC" in
  clio-kit==*)
    JARVIS_MCP_VERSION="${{JARVIS_MCP_INSTALL_SPEC#clio-kit==}}"
    COMPONENT_DOWNLOAD_DIR="$HOME/.local/share/clio-relay/component-wheels/clio-kit"
    rm -rf "$COMPONENT_DOWNLOAD_DIR"
    mkdir -p "$COMPONENT_DOWNLOAD_DIR"
    python -m pip download --disable-pip-version-check --no-cache-dir \
      --index-url https://pypi.org/simple --no-deps --only-binary=:all: \
      --dest "$COMPONENT_DOWNLOAD_DIR" "$JARVIS_MCP_INSTALL_SPEC"
    mapfile -t JARVIS_MCP_WHEELS < <(
      find "$COMPONENT_DOWNLOAD_DIR" -maxdepth 1 -type f -name 'clio_kit-*.whl' -print
    )
    if [ "${{#JARVIS_MCP_WHEELS[@]}}" -ne 1 ]; then
      echo "expected exactly one downloaded clio-kit wheel" >&2
      exit 1
    fi
    JARVIS_MCP_ARTIFACT_PATH="${{JARVIS_MCP_WHEELS[0]}}"
    JARVIS_MCP_INSTALL_TARGET="$JARVIS_MCP_ARTIFACT_PATH"
    JARVIS_MCP_REQUESTED_SOURCE="pypi"
    ;;
  *.whl)
    test -f "$JARVIS_MCP_INSTALL_SPEC"
    COMPONENT_DOWNLOAD_DIR="$HOME/.local/share/clio-relay/component-wheels/clio-kit"
    mkdir -p "$(dirname "$COMPONENT_DOWNLOAD_DIR")"
    COMPONENT_STAGING="$(mktemp "${{COMPONENT_DOWNLOAD_DIR}}.XXXXXX.whl")"
    cp "$JARVIS_MCP_INSTALL_SPEC" "$COMPONENT_STAGING"
    rm -rf "$COMPONENT_DOWNLOAD_DIR"
    mkdir -p "$COMPONENT_DOWNLOAD_DIR"
    JARVIS_MCP_ARTIFACT_PATH="$COMPONENT_DOWNLOAD_DIR/$(basename "$JARVIS_MCP_INSTALL_SPEC")"
    mv "$COMPONENT_STAGING" "$JARVIS_MCP_ARTIFACT_PATH"
    JARVIS_MCP_INSTALL_TARGET="$JARVIS_MCP_ARTIFACT_PATH"
    JARVIS_MCP_REQUESTED_SOURCE="wheel"
    ;;
  *)
    echo "clio-kit bootstrap source must be an exact version or wheel" >&2
    exit 1
    ;;
esac
deactivate
uvx --refresh --no-config --from "$JARVIS_MCP_INSTALL_TARGET" clio-kit --help >/dev/null

DEST="$HOME/.local/src/clio-relay"
rm -rf "$DEST"
mkdir -p "$DEST"
tar -xf /tmp/clio-relay-head.tar -C "$DEST"
uv venv --python 3.12 --seed --clear "$HOME/.local/share/clio-relay/relay-venv312"
. "$HOME/.local/share/clio-relay/relay-venv312/bin/activate"
RELAY_INSTALL_SPEC={rendered_relay_install_spec}
RELAY_INSTALL_TARGET="$RELAY_INSTALL_SPEC"
RELAY_ARTIFACT_PATH=""
case "$RELAY_INSTALL_SPEC" in
  clio-relay==*)
    DOWNLOAD_DIR="$DEST/downloaded-wheels"
    rm -rf "$DOWNLOAD_DIR"
    mkdir -p "$DOWNLOAD_DIR"
    python -m pip download --disable-pip-version-check --no-deps --only-binary=:all: \
      --dest "$DOWNLOAD_DIR" "$RELAY_INSTALL_SPEC"
    mapfile -t RELAY_WHEELS < <(
      find "$DOWNLOAD_DIR" -maxdepth 1 -type f -name 'clio_relay-*.whl' -print
    )
    if [ "${{#RELAY_WHEELS[@]}}" -ne 1 ]; then
      echo "expected exactly one downloaded clio-relay wheel" >&2
      exit 1
    fi
    RELAY_ARTIFACT_PATH="${{RELAY_WHEELS[0]}}"
    RELAY_INSTALL_TARGET="$RELAY_ARTIFACT_PATH"
    ;;
  *.whl)
    RELAY_ARTIFACT_PATH="$RELAY_INSTALL_SPEC"
    ;;
esac
uv pip install --refresh-package clio-relay "$RELAY_INSTALL_TARGET"
uv pip install --no-deps --refresh-package jarvis-cd "$JARVIS_CD_WHEEL"
uv pip install --python "$JARVIS_VENV/bin/python" \\
  --refresh-package clio-relay "$RELAY_INSTALL_TARGET"
"$JARVIS_VENV/bin/python" -c 'import clio_relay, jarvis_cd'
verify_jarvis_cd_distribution() {{
  local interpreter="$1"
  "$interpreter" - \\
    "$JARVIS_CD_WHEEL" \\
    "$JARVIS_CD_WHEEL_SHA256" \\
    "$JARVIS_CD_VERSION" \\
    <<'__CLIO_RELAY_PROGRESS_PROVIDER_PROBE__'
import hashlib
import json
import sys
from importlib.metadata import distribution
from pathlib import Path

wheel = Path(sys.argv[1]).resolve()
expected_sha256 = sys.argv[2]
expected_version = sys.argv[3]
if hashlib.sha256(wheel.read_bytes()).hexdigest() != expected_sha256:
    raise SystemExit("JARVIS-CD release wheel digest changed after installation")
installed = distribution("jarvis_cd")
if installed.version != expected_version:
    raise SystemExit("JARVIS-CD installed version does not match the release pin")
direct_url = json.loads(installed.read_text("direct_url.json") or "{{}}")
if direct_url.get("url") != wheel.as_uri():
    raise SystemExit("JARVIS-CD was not installed from the verified release wheel")
entry_points = [
    entry_point
    for entry_point in installed.entry_points
    if entry_point.group == "clio_relay.package_progress_adapters"
]
if not entry_points:
    raise SystemExit("jarvis-cd exposes no package progress provider entry points")
print(f"jarvis_cd_progress_provider={{installed.name}}=={{installed.version}}")
__CLIO_RELAY_PROGRESS_PROVIDER_PROBE__
}}
verify_jarvis_cd_distribution python
verify_jarvis_cd_distribution "$JARVIS_VENV/bin/python"
export CLIO_RELAY_BOOTSTRAP_INSTALL_SPEC="$RELAY_INSTALL_SPEC"
export CLIO_RELAY_BOOTSTRAP_ARTIFACT="$RELAY_ARTIFACT_PATH"
export CLIO_RELAY_BOOTSTRAP_JARVIS_UTIL_COMMIT="$JARVIS_UTIL_COMMIT"
export CLIO_RELAY_BOOTSTRAP_JARVIS_CD_VERSION="$JARVIS_CD_VERSION"
export CLIO_RELAY_BOOTSTRAP_JARVIS_CD_WHEEL_URL="$JARVIS_CD_WHEEL_URL"
export CLIO_RELAY_BOOTSTRAP_JARVIS_CD_WHEEL="$JARVIS_CD_WHEEL"
export CLIO_RELAY_BOOTSTRAP_JARVIS_CD_WHEEL_SHA256="$JARVIS_CD_WHEEL_SHA256"
export CLIO_RELAY_BOOTSTRAP_JARVIS_CD_EXECUTION_PYTHON="$JARVIS_VENV/bin/python"
export CLIO_RELAY_BOOTSTRAP_JARVIS_EXECUTABLE="$HOME/.local/bin/jarvis"
export CLIO_RELAY_BOOTSTRAP_JARVIS_MCP_INSTALL_SPEC="$JARVIS_MCP_INSTALL_SPEC"
export CLIO_RELAY_BOOTSTRAP_JARVIS_MCP_ARTIFACT="$JARVIS_MCP_ARTIFACT_PATH"
export CLIO_RELAY_BOOTSTRAP_JARVIS_MCP_SOURCE="$JARVIS_MCP_REQUESTED_SOURCE"
export CLIO_RELAY_BOOTSTRAP_JARVIS_MCP_VERSION="$JARVIS_MCP_VERSION"
python - <<'__CLIO_RELAY_INSTALL_RECEIPT__'
import os
import sys
from importlib.metadata import distribution
from pathlib import Path

from clio_relay.installation import ComponentArtifactIdentity, write_install_receipt
from clio_relay.validation_report import sha256_file

artifact_value = os.environ["CLIO_RELAY_BOOTSTRAP_ARTIFACT"]
component_artifact_value = os.environ["CLIO_RELAY_BOOTSTRAP_JARVIS_MCP_ARTIFACT"]
component_artifact = Path(component_artifact_value).resolve() if component_artifact_value else None
component_version = os.environ["CLIO_RELAY_BOOTSTRAP_JARVIS_MCP_VERSION"] or None
component_spec = os.environ["CLIO_RELAY_BOOTSTRAP_JARVIS_MCP_INSTALL_SPEC"]
jarvis_cd_wheel = Path(os.environ["CLIO_RELAY_BOOTSTRAP_JARVIS_CD_WHEEL"]).resolve()
jarvis_cd_wheel_sha256 = os.environ["CLIO_RELAY_BOOTSTRAP_JARVIS_CD_WHEEL_SHA256"]
if sha256_file(jarvis_cd_wheel) != jarvis_cd_wheel_sha256:
    raise SystemExit("jarvis-cd receipt wheel digest does not match bootstrap pin")
jarvis_cd_distribution = distribution("jarvis_cd")
if jarvis_cd_distribution.version != os.environ["CLIO_RELAY_BOOTSTRAP_JARVIS_CD_VERSION"]:
    raise SystemExit("jarvis-cd receipt version does not match the released wheel pin")
jarvis_cd_entry_points = sorted(
    f"{{entry_point.group}}:{{entry_point.name}}"
    for entry_point in jarvis_cd_distribution.entry_points
    if entry_point.group == "clio_relay.package_progress_adapters"
)
if not jarvis_cd_entry_points:
    raise SystemExit("jarvis-cd receipt has no package progress provider entry points")
runtime_command = (
    [
        str(Path.home() / ".local" / "bin" / "uvx"),
        "--refresh",
        "--no-config",
        "--from",
        str(component_artifact),
        "clio-kit",
        "mcp-server",
        "jarvis",
    ]
    if component_artifact is not None
    else []
)
receipt = write_install_receipt(
    install_spec=os.environ["CLIO_RELAY_BOOTSTRAP_INSTALL_SPEC"],
    artifact_path=Path(artifact_value) if artifact_value else None,
    components={{
        "clio-kit": component_version or component_spec,
        "jarvis-cd": os.environ["CLIO_RELAY_BOOTSTRAP_JARVIS_CD_VERSION"],
        "jarvis-util": os.environ["CLIO_RELAY_BOOTSTRAP_JARVIS_UTIL_COMMIT"],
    }},
    component_artifacts={{
        "clio-kit": ComponentArtifactIdentity(
            distribution="clio-kit",
            distribution_version=component_version,
            install_spec=component_spec,
            requested_source=os.environ["CLIO_RELAY_BOOTSTRAP_JARVIS_MCP_SOURCE"],
            artifact_filename=(component_artifact.name if component_artifact else None),
            artifact_sha256=(sha256_file(component_artifact) if component_artifact else None),
            runtime_artifact_path=(str(component_artifact) if component_artifact else None),
            runtime_command=runtime_command,
        ),
        "jarvis-cd": ComponentArtifactIdentity(
            distribution=jarvis_cd_distribution.name,
            distribution_version=jarvis_cd_distribution.version,
            install_spec=os.environ["CLIO_RELAY_BOOTSTRAP_JARVIS_CD_WHEEL_URL"],
            requested_source="github_release",
            artifact_filename=jarvis_cd_wheel.name,
            artifact_sha256=jarvis_cd_wheel_sha256,
            runtime_artifact_path=str(jarvis_cd_wheel),
            runtime_command=[
                os.environ["CLIO_RELAY_BOOTSTRAP_JARVIS_EXECUTABLE"],
                "--help",
            ],
            runtime_interpreters={{
                "provider": sys.executable,
                "execution": os.environ["CLIO_RELAY_BOOTSTRAP_JARVIS_CD_EXECUTION_PYTHON"],
            }},
            runtime_executables={{
                "jarvis": os.environ["CLIO_RELAY_BOOTSTRAP_JARVIS_EXECUTABLE"],
            }},
            entry_points=jarvis_cd_entry_points,
        ),
    }},
)
print(f"relay_install_receipt={{receipt.schema_version}}")
print(f"relay_artifact_sha256={{receipt.artifact_sha256 or 'none'}}")
__CLIO_RELAY_INSTALL_RECEIPT__
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
CLIO_RELAY_AGENT_BIN="${{AGENT_BIN:-agent}}" \
CLIO_RELAY_AGENT_ADAPTER={rendered_agent_adapter} \
CLIO_RELAY_AGENT_ARGS={rendered_agent_args} \
clio-relay init

python3 - <<'__CLIO_RELAY_BOOTSTRAP_RECEIPT__'
import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path

install_receipt = Path.home() / ".local/share/clio-relay/install-receipt.json"
install_receipt_sha256 = hashlib.sha256(install_receipt.read_bytes()).hexdigest()
receipt = {{
    "schema_version": "clio-relay.bootstrap-receipt.v1",
    "invocation_id": {invocation_id!r},
    "bootstrap_profile": "linux-user",
    "relay_install_spec": {relay_install_spec!r},
    "install_receipt_sha256": install_receipt_sha256,
    "completed_at": datetime.now(timezone.utc).isoformat(),
}}
destination = Path.home() / ".local/share/clio-relay/bootstrap-receipt.json"
temporary = destination.with_name(f".{{destination.name}}.{{os.getpid()}}.tmp")
temporary.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n", encoding="utf-8")
os.chmod(temporary, 0o600)
os.replace(temporary, destination)
print(f"bootstrap_receipt={{destination}}")
print(f"bootstrap_invocation_id={{receipt['invocation_id']}}")
print(f"bootstrap_install_receipt_sha256={{install_receipt_sha256}}")
__CLIO_RELAY_BOOTSTRAP_RECEIPT__

echo "frpc=$("$HOME/.local/bin/frpc" --version)"
echo "frps=$("$HOME/.local/bin/frps" --version)"
if [ -x "$AGENT_BIN" ]; then
  echo "agent=$("$AGENT_BIN" --version)"
fi
echo "jarvis=$("$HOME/.local/bin/jarvis" --help | head -n 1)"
echo "relay=$(clio-relay --help | head -n 1)"
"""
    return script.replace("\r\n", "\n")


def _render_relay_install_spec(relay_install_spec: str) -> str:
    if relay_install_spec == "$DEST":
        return '"$DEST"'
    if relay_install_spec.startswith("$DEST/"):
        return '"$DEST"/' + shlex.quote(relay_install_spec.removeprefix("$DEST/"))
    return shlex.quote(relay_install_spec)


def create_bootstrap_archive(
    *,
    source_root: Path,
    archive: Path,
    relay_wheel: Path | None = None,
) -> BootstrapArchive:
    """Create the archive used by remote bootstrap.

    A clean git checkout deploys that exact committed tree. Installed-package
    runs deploy packaged JARVIS assets and install either the supplied candidate
    wheel or the exact package version, so bootstrap does not require a checkout.
    """
    if relay_wheel is not None:
        _write_packaged_bootstrap_archive(archive, relay_wheel=relay_wheel)
        return BootstrapArchive(
            archive=archive,
            install_spec=f"$DEST/wheels/{relay_wheel.name}",
        )
    if _is_clio_relay_git_checkout(source_root):
        assert_clean_git_checkout(source_root)
        _run(["git", "archive", "--format=tar", "-o", str(archive), "HEAD"], cwd=source_root)
        return BootstrapArchive(archive=archive, install_spec="$DEST")
    _write_packaged_bootstrap_archive(archive, relay_wheel=None)
    return BootstrapArchive(archive=archive, install_spec=f"clio-relay=={__version__}")


def _write_packaged_bootstrap_archive(archive: Path, *, relay_wheel: Path | None) -> None:
    if relay_wheel is not None and not relay_wheel.is_file():
        raise ConfigurationError(f"relay wheel does not exist: {relay_wheel}")
    assets = resources.files("clio_relay").joinpath("assets", "jarvis-packages")
    source_assets = Path(__file__).resolve().parents[2] / "jarvis-packages"
    with tarfile.open(archive, "w") as tar:
        if relay_wheel is not None:
            tar.add(relay_wheel, arcname=str(Path("wheels", relay_wheel.name)))
        if assets.is_dir():
            with resources.as_file(assets) as asset_path:
                _add_jarvis_assets_to_archive(tar=tar, asset_path=asset_path)
            return
        if source_assets.is_dir():
            _add_jarvis_assets_to_archive(tar=tar, asset_path=source_assets)
            return
    raise ConfigurationError("installed clio-relay package does not include jarvis package assets")


def _is_clio_relay_git_checkout(source_root: Path) -> bool:
    pyproject = source_root / "pyproject.toml"
    if not (source_root / ".git").exists() or not pyproject.exists():
        return False
    try:
        text = pyproject.read_text(encoding="utf-8")
    except OSError:
        return False
    return 'name = "clio-relay"' in text


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
