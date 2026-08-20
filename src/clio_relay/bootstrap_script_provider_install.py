"""Rendered-script fragment: pinned uv/frp/jarvis-cd provider install and verification.

Split from bootstrap.py (clio-relay#255) -- one sequential fragment of the Linux cluster
bootstrap's rendered shell script. Pure string assembly, called only from bootstrap.py's
own renderer; not independently monkeypatched.
"""

from __future__ import annotations

from clio_relay.bootstrap_constants import (
    FRP_LINUX_AMD64_SHA256,
    FRPC_LINUX_AMD64_SHA256,
    FRPS_LINUX_AMD64_SHA256,
    JARVIS_CD_VERSION,
    JARVIS_CD_WHEEL_FILENAME,
    JARVIS_CD_WHEEL_SHA256,
    JARVIS_CD_WHEEL_URL,
    JARVIS_UTIL_COMMIT,
    UV_LINUX_AMD64_ARCHIVE_SHA256,
    UV_LINUX_AMD64_EXECUTABLE_SHA256,
    UV_VERSION,
)
from clio_relay.jarvis_mcp import (
    CLIO_KIT_JARVIS_MCP_VERSION,
    CLIO_KIT_JARVIS_MCP_WHEEL_FILENAME,
    CLIO_KIT_JARVIS_MCP_WHEEL_URL,
)


def script_provider_install(
    *,
    artifact_fetch_function: str,
    candidate_bounded_process_sha256: str,
    candidate_errors_sha256: str,
    candidate_process_containment_sha256: str,
    candidate_provider_build_info_sha256: str,
    candidate_reconcile_sha256: str,
    candidate_safe_archive_sha256: str,
    candidate_uv_install_program: str,
    cluster: str | None,
    frp_version: str,
    init_command: str,
    invocation_id: str,
    ownership_proof_adoption_python: str,
    pinned_uv_copy_program: str,
    preparing_root_program: str,
    relay_only_reconcile: str,
    rendered_agent_adapter: str,
    rendered_agent_args: str,
    rendered_agent_npm_bin: str,
    rendered_agent_npm_package: str,
    rendered_allow_jarvis_resource_graph_build: str,
    rendered_bootstrap_journal_source: str,
    rendered_candidate_package_sources: str,
    rendered_candidate_relay_install_spec: str,
    rendered_core_dir: str,
    rendered_desired_state: str,
    rendered_jarvis_mcp_artifact_sha256: str,
    rendered_jarvis_mcp_install_spec: str,
    rendered_jarvis_resource_graph_profile: str,
    rendered_relay_artifact_sha256: str,
    rendered_relay_install_spec: str,
    rendered_source_archive: str,
    rendered_source_archive_sha256: str,
    rendered_spool_dir: str,
    shared_directory_mkdir_owned_helper: str,
    stable_activation_link_adoption: str,
    worker_fence: str,
    worker_recheck: str,
    worker_restart: str,
    worker_service: str,
) -> str:
    """Render: pinned uv/frp/jarvis-cd provider install and verification."""
    return f"""value = json.loads(os.environ["BOOTSTRAP_DESIRED_STATE"])
value["agent_npm_package"] = os.environ["AGENT_NPM_PACKAGE"] or None
value["agent_npm_bin"] = os.environ["AGENT_NPM_BIN"] or None
payload = json.dumps(
    value,
    ensure_ascii=True,
    allow_nan=False,
    sort_keys=True,
    separators=(",", ":"),
).encode("utf-8")
print(hashlib.sha256(payload).hexdigest())
__CLIO_RELAY_FRESH_DESIRED_FINGERPRINT__
)"
WORKER_SERVICE_NAME="$(
  python3 - <<'__CLIO_RELAY_FRESH_WORKER_SERVICE__'
import json
import os

print(json.loads(os.environ["BOOTSTRAP_DESIRED_STATE"])["worker_service"] or "")
__CLIO_RELAY_FRESH_WORKER_SERVICE__
)"
BOOTSTRAP_SERVICE_ACTIVE_BEFORE=unknown
BOOTSTRAP_SERVICE_ENABLED_BEFORE=unknown
if [ -n "$WORKER_SERVICE_NAME" ]; then
  if systemctl --user is-active --quiet "$WORKER_SERVICE_NAME"; then
    BOOTSTRAP_SERVICE_ACTIVE_BEFORE=true
  else
    BOOTSTRAP_SERVICE_ACTIVE_BEFORE=false
  fi
  if systemctl --user is-enabled --quiet "$WORKER_SERVICE_NAME"; then
    BOOTSTRAP_SERVICE_ENABLED_BEFORE=true
  else
    BOOTSTRAP_SERVICE_ENABLED_BEFORE=false
  fi
fi
BOOTSTRAP_GENERATION="$HOME/.local/share/clio-relay/generations/$BOOTSTRAP_DESIRED_FINGERPRINT"
BOOTSTRAP_TRANSACTION_ROOT="$HOME/.local/share/clio-relay/transactions/$BOOTSTRAP_INVOCATION_ID"
BOOTSTRAP_OWNED_PATHS_JSON="$(
  python3 - "$BOOTSTRAP_DESIRED_FINGERPRINT" "$BOOTSTRAP_INVOCATION_ID" \
    "$JARVIS_STAGING_MODE" "$ACTIVATION_STAGING_MODE" \
    <<'__CLIO_RELAY_FRESH_OWNERSHIP__'
import hashlib
import json
import os
import stat
import subprocess
import sys
from pathlib import Path

home = Path.home()
fingerprint = sys.argv[1]
invocation_id = sys.argv[2]
jarvis_staging_mode = sys.argv[3] == "1"
activation_staging_mode = sys.argv[4] == "1"

def classify(path: Path) -> os.stat_result | None:
    try:
        return path.lstat()
    except FileNotFoundError:
        return None

def require_regular_executable(path: Path, expected_sha256: str | None = None) -> None:
    details = classify(path)
    if details is None or not stat.S_ISREG(details.st_mode) or not os.access(path, os.X_OK):
        raise SystemExit(f"bootstrap cannot adopt an existing executable: {{path}}")
    if expected_sha256 is not None:
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        if digest != expected_sha256:
            raise SystemExit(f"bootstrap existing executable digest changed: {{path}}")

owned: dict[str, dict[str, str]] = {{}}

def absent(name: str, path: Path, kind: str) -> None:
    if classify(path) is not None:
        raise SystemExit(f"fresh bootstrap refuses a preexisting mutation target: {{path}}")
    owned[name] = {{"path": str(path), "kind": kind}}

frpc = home / ".local/bin/frpc"
frps = home / ".local/bin/frps"
if classify(frpc) is None and classify(frps) is None:
    owned["frpc"] = {{"path": str(frpc), "kind": "file"}}
    owned["frps"] = {{"path": str(frps), "kind": "file"}}
else:
    require_regular_executable(frpc, "{FRPC_LINUX_AMD64_SHA256}")
    require_regular_executable(frps, "{FRPS_LINUX_AMD64_SHA256}")

uv = home / ".local/bin/uv"
uvx = home / ".local/bin/uvx"
if classify(uv) is None and classify(uvx) is None:
    owned["uv"] = {{"path": str(uv), "kind": "file"}}
    owned["uvx"] = {{"path": str(uvx), "kind": "file"}}
else:
    require_regular_executable(uv, "{UV_LINUX_AMD64_EXECUTABLE_SHA256}")
    require_regular_executable(uvx)
    completed = subprocess.run(
        [str(uv), "--version"],
        check=False,
        capture_output=True,
        text=True,
        timeout=10,
    )
    # uv prints "uv <version> (<platform> <date>)"; older builds printed just
    # "uv <version>". Compare the version TOKEN, never the whole line, or a
    # byte-identical pinned uv is rejected over a cosmetic suffix (#158).
    version_fields = completed.stdout.strip().split()
    if completed.returncode != 0 or version_fields[:2] != ["uv", "{UV_VERSION}"]:
        raise SystemExit("bootstrap cannot adopt an existing uv version")

jarvis_util = home / ".local/src/jarvis-util"
if classify(jarvis_util) is None:
    owned["jarvis_util"] = {{"path": str(jarvis_util), "kind": "directory"}}
else:
    if jarvis_util.is_symlink() or not (jarvis_util / ".git").is_dir():
        raise SystemExit("bootstrap cannot adopt the existing jarvis-util path")
    commit = subprocess.run(
        ["git", "-C", str(jarvis_util), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
        timeout=15,
    ).stdout.strip()
    status = subprocess.run(
        ["git", "-C", str(jarvis_util), "status", "--porcelain=v1", "--untracked-files=all"],
        check=True,
        capture_output=True,
        text=True,
        timeout=15,
    ).stdout
    if commit != "{JARVIS_UTIL_COMMIT}" or status:
        raise SystemExit("bootstrap cannot mutate an existing jarvis-util checkout")

jarvis_venv_entry = (
    # clio-relay#254: a staging transaction never claims the LIVE jarvis-venv
    # as absent -- it owns only the staged replacement, promoted in later by
    # one atomic exchange that never requires the live path to be absent.
    (
        "jarvis_venv_staged",
        home / (".local/share/clio-relay/jarvis-venv.staging-" + invocation_id),
        "directory",
    )
    if jarvis_staging_mode
    else ("jarvis_venv", home / ".local/share/clio-relay/jarvis-venv", "directory")
)
{ownership_proof_adoption_python}

print(json.dumps(owned, sort_keys=True, separators=(",", ":")))
__CLIO_RELAY_FRESH_OWNERSHIP__
)"
export BOOTSTRAP_INVOCATION_ID BOOTSTRAP_DESIRED_FINGERPRINT
export BOOTSTRAP_GENERATION BOOTSTRAP_TRANSACTION_ROOT WORKER_SERVICE_NAME
export BOOTSTRAP_SERVICE_ACTIVE_BEFORE BOOTSTRAP_SERVICE_ENABLED_BEFORE
bootstrap_journal_action create \
  "$BOOTSTRAP_TRANSACTION_JOURNAL" \
  "$BOOTSTRAP_INVOCATION_ID" \
  "$BOOTSTRAP_DESIRED_FINGERPRINT" \
  full \
  "$WORKER_SERVICE_NAME" \
  "$BOOTSTRAP_SERVICE_ACTIVE_BEFORE" \
  "$BOOTSTRAP_SERVICE_ENABLED_BEFORE" \
  "$BOOTSTRAP_OWNED_PATHS_JSON"
BOOTSTRAP_OWNERSHIP_IDENTITY="$(
  BOOTSTRAP_OWNED_PATHS_JSON="$BOOTSTRAP_OWNED_PATHS_JSON" \
    python3 - <<'__CLIO_RELAY_FRESH_OWNERSHIP_IDENTITY__'
import hashlib
import json
import os

value = json.loads(os.environ["BOOTSTRAP_OWNED_PATHS_JSON"])
payload = json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
print(hashlib.sha256(payload).hexdigest())
__CLIO_RELAY_FRESH_OWNERSHIP_IDENTITY__
)"
bootstrap_journal_action phase "$BOOTSTRAP_TRANSACTION_JOURNAL" \
  ownership_manifest "$BOOTSTRAP_OWNERSHIP_IDENTITY"
bootstrap_journal_action advance "$BOOTSTRAP_TRANSACTION_JOURNAL" inspected
bootstrap_journal_action advance "$BOOTSTRAP_TRANSACTION_JOURNAL" fencing
{worker_fence}
bootstrap_journal_action advance "$BOOTSTRAP_TRANSACTION_JOURNAL" fenced
bootstrap_journal_action advance "$BOOTSTRAP_TRANSACTION_JOURNAL" preparing
mkdir -m 0700 -p \
  "$HOME/.local/share/clio-relay/transactions" \
  "$HOME/.local/share/clio-relay/component-wheels" \
  "$HOME/.local/share/clio-relay/generations"
bootstrap_journal_action mkdir-owned "$BOOTSTRAP_TRANSACTION_JOURNAL" transaction_root
mkdir -m 0700 "$BOOTSTRAP_TRANSACTION_ROOT/downloads"
mkdir -m 0700 "$BOOTSTRAP_TRANSACTION_ROOT/uv-cache"
export UV_CACHE_DIR="$BOOTSTRAP_TRANSACTION_ROOT/uv-cache"
bootstrap_journal_action mkdir-owned "$BOOTSTRAP_TRANSACTION_JOURNAL" generation
BOOTSTRAP_FULL_PREPARE_STARTED_NS="$(python3 -c 'import time; print(time.monotonic_ns())')"
BOOTSTRAP_FRP_DOWNLOADED=0
BOOTSTRAP_UV_DOWNLOADED=0
BOOTSTRAP_JARVIS_UTIL_DOWNLOADED=0
BOOTSTRAP_JARVIS_CD_DOWNLOADED=0
BOOTSTRAP_CLIO_KIT_DOWNLOADED=0
BOOTSTRAP_RELAY_DOWNLOAD_COUNT=0

cd "$BOOTSTRAP_TRANSACTION_ROOT/downloads"
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
  install -m 0755 "frp_${{FRP_VERSION}}_linux_amd64/frpc" \
    "$BOOTSTRAP_TRANSACTION_ROOT/downloads/frpc.install"
  install -m 0755 "frp_${{FRP_VERSION}}_linux_amd64/frps" \
    "$BOOTSTRAP_TRANSACTION_ROOT/downloads/frps.install"
  bootstrap_journal_action copy-owned "$BOOTSTRAP_TRANSACTION_JOURNAL" frpc \
    "$BOOTSTRAP_TRANSACTION_ROOT/downloads/frpc.install" 0755
  bootstrap_journal_action copy-owned "$BOOTSTRAP_TRANSACTION_JOURNAL" frps \
    "$BOOTSTRAP_TRANSACTION_ROOT/downloads/frps.install" 0755
  echo "$FRPC_SHA256 *$HOME/.local/bin/frpc" | sha256sum --check --strict -
  echo "$FRPS_SHA256 *$HOME/.local/bin/frps" | sha256sum --check --strict -
  BOOTSTRAP_FRP_DOWNLOADED=1
fi

UV_VERSION="{UV_VERSION}"
UV_ARCHIVE_SHA256="{UV_LINUX_AMD64_ARCHIVE_SHA256}"
UV_EXECUTABLE_SHA256="{UV_LINUX_AMD64_EXECUTABLE_SHA256}"
UV_ARCHIVE="uv-x86_64-unknown-linux-gnu.tar.gz"
if [ ! -x "$HOME/.local/bin/uv" ] \
  || [ "$("$HOME/.local/bin/uv" --version | awk '{{print $1 " " $2}}')" != "uv $UV_VERSION" ] \
  || ! echo "$UV_EXECUTABLE_SHA256 *$HOME/.local/bin/uv" | \
       sha256sum --check --status -; then
  curl -L --fail --retry 3 -o "$UV_ARCHIVE" \
    "https://github.com/astral-sh/uv/releases/download/$UV_VERSION/$UV_ARCHIVE"
  echo "$UV_ARCHIVE_SHA256 *$UV_ARCHIVE" | sha256sum --check --strict -
  tar -xzf "$UV_ARCHIVE"
  install -m 0755 "uv-x86_64-unknown-linux-gnu/uv" \
    "$BOOTSTRAP_TRANSACTION_ROOT/downloads/uv.install"
  install -m 0755 "uv-x86_64-unknown-linux-gnu/uvx" \
    "$BOOTSTRAP_TRANSACTION_ROOT/downloads/uvx.install"
  bootstrap_journal_action copy-owned "$BOOTSTRAP_TRANSACTION_JOURNAL" uv \
    "$BOOTSTRAP_TRANSACTION_ROOT/downloads/uv.install" 0755
  bootstrap_journal_action copy-owned "$BOOTSTRAP_TRANSACTION_JOURNAL" uvx \
    "$BOOTSTRAP_TRANSACTION_ROOT/downloads/uvx.install" 0755
  echo "$UV_EXECUTABLE_SHA256 *$HOME/.local/bin/uv" | sha256sum --check --strict -
  BOOTSTRAP_UV_DOWNLOADED=1
fi
{shared_directory_mkdir_owned_helper}
bootstrap_mkdir_owned_if_absent "$HOME/.local/share/clio-relay/uv-python" uv_python
uv python install 3.12

if [ ! -x "$AGENT_BIN" ] && [ -n "$AGENT_NPM_PACKAGE" ] && command -v npm >/dev/null 2>&1; then
  npm install -g "$AGENT_NPM_PACKAGE"
fi

if [ "$JARVIS_STAGING_MODE" = "1" ]; then
  JARVIS_VENV="$JARVIS_VENV_STAGED_PATH"
  bootstrap_journal_action mkdir-owned "$BOOTSTRAP_TRANSACTION_JOURNAL" jarvis_venv_staged
else
  JARVIS_VENV="$JARVIS_VENV_LIVE_PATH"
  bootstrap_journal_action mkdir-owned "$BOOTSTRAP_TRANSACTION_JOURNAL" jarvis_venv
fi
uv venv --python 3.12 --seed "$JARVIS_VENV"
. "$JARVIS_VENV/bin/activate"
JARVIS_UTIL_COMMIT="{JARVIS_UTIL_COMMIT}"
if [ ! -d "$HOME/.local/src/jarvis-util/.git" ]; then
  bootstrap_journal_action mkdir-owned "$BOOTSTRAP_TRANSACTION_JOURNAL" jarvis_util
  git clone --no-checkout https://github.com/grc-iit/jarvis-util.git \
    "$HOME/.local/src/jarvis-util"
  git -C "$HOME/.local/src/jarvis-util" fetch --depth 1 origin "$JARVIS_UTIL_COMMIT"
  BOOTSTRAP_JARVIS_UTIL_DOWNLOADED=1
  git -C "$HOME/.local/src/jarvis-util" checkout --detach "$JARVIS_UTIL_COMMIT"
else
  test "$(git -C "$HOME/.local/src/jarvis-util" rev-parse HEAD)" = \
    "$JARVIS_UTIL_COMMIT"
  test -z "$(
    git -C "$HOME/.local/src/jarvis-util" status --porcelain=v1 --untracked-files=all
  )"
fi
test "$(git -C "$HOME/.local/src/jarvis-util" rev-parse HEAD)" = "$JARVIS_UTIL_COMMIT"
python -m pip install --isolated --index-url https://pypi.org/simple \\
  -r "$HOME/.local/src/jarvis-util/requirements.txt"
python -m pip install --isolated --no-deps "$HOME/.local/src/jarvis-util"
JARVIS_CD_VERSION="{JARVIS_CD_VERSION}"
JARVIS_CD_WHEEL_URL="{JARVIS_CD_WHEEL_URL}"
JARVIS_CD_WHEEL_SHA256="{JARVIS_CD_WHEEL_SHA256}"
JARVIS_CD_WHEEL_DIR="$HOME/.local/share/clio-relay/component-wheels/jarvis-cd"
JARVIS_CD_WHEEL="$JARVIS_CD_WHEEL_DIR/{JARVIS_CD_WHEEL_FILENAME}"
mkdir -m 0700 -p "$(dirname "$JARVIS_CD_WHEEL_DIR")"
bootstrap_mkdir_owned_if_absent "$JARVIS_CD_WHEEL_DIR" jarvis_cd_wheels
JARVIS_CD_STAGING="$(mktemp "${{JARVIS_CD_WHEEL}}.XXXXXX")"
bootstrap_fetch_exact_artifact \\
  "$JARVIS_CD_WHEEL_URL" "$JARVIS_CD_WHEEL_SHA256" "$JARVIS_CD_STAGING"
BOOTSTRAP_JARVIS_CD_DOWNLOADED=1
echo "$JARVIS_CD_WHEEL_SHA256 *$JARVIS_CD_STAGING" | sha256sum --check --strict -
mv "$JARVIS_CD_STAGING" "$JARVIS_CD_WHEEL"
python -m pip install --isolated --index-url https://pypi.org/simple "$JARVIS_CD_WHEEL"
JARVIS_MCP_INSTALL_SPEC={rendered_jarvis_mcp_install_spec}
JARVIS_MCP_ARTIFACT_SHA256={rendered_jarvis_mcp_artifact_sha256}
JARVIS_MCP_INSTALL_TARGET="$JARVIS_MCP_INSTALL_SPEC"
JARVIS_MCP_ARTIFACT_PATH=""
JARVIS_MCP_REQUESTED_SOURCE="checkout"
JARVIS_MCP_VERSION=""
bootstrap_mkdir_owned_if_absent \
  "$HOME/.local/share/clio-relay/component-wheels/clio-kit" clio_kit_wheels
case "$JARVIS_MCP_INSTALL_SPEC" in
  "{CLIO_KIT_JARVIS_MCP_WHEEL_URL}")
    JARVIS_MCP_VERSION="{CLIO_KIT_JARVIS_MCP_VERSION}"
    COMPONENT_DOWNLOAD_DIR="$HOME/.local/share/clio-relay/component-wheels/clio-kit"
    test -d "$COMPONENT_DOWNLOAD_DIR"
    JARVIS_MCP_ARTIFACT_PATH="$COMPONENT_DOWNLOAD_DIR/{CLIO_KIT_JARVIS_MCP_WHEEL_FILENAME}"
    COMPONENT_STAGING="$(mktemp "${{JARVIS_MCP_ARTIFACT_PATH}}.XXXXXX")"
    curl --fail --location --proto '=https' --proto-redir '=https' --tlsv1.2 \
      --retry 3 --retry-all-errors --retry-max-time 180 \
      --connect-timeout 20 --max-time 180 \
      --output "$COMPONENT_STAGING" "$JARVIS_MCP_INSTALL_SPEC"
    echo "$JARVIS_MCP_ARTIFACT_SHA256 *$COMPONENT_STAGING" | \
      sha256sum --check --strict -
    mv "$COMPONENT_STAGING" "$JARVIS_MCP_ARTIFACT_PATH"
    JARVIS_MCP_INSTALL_TARGET="$JARVIS_MCP_ARTIFACT_PATH"
    JARVIS_MCP_REQUESTED_SOURCE="github_release"
    BOOTSTRAP_CLIO_KIT_DOWNLOADED=1
    ;;
  clio-kit==*)
    JARVIS_MCP_VERSION="${{JARVIS_MCP_INSTALL_SPEC#clio-kit==}}"
    COMPONENT_DOWNLOAD_DIR="$HOME/.local/share/clio-relay/component-wheels/clio-kit"
    test -d "$COMPONENT_DOWNLOAD_DIR"
    python -m pip download --isolated --disable-pip-version-check --no-cache-dir \
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
    BOOTSTRAP_CLIO_KIT_DOWNLOADED=1
    ;;
  *.whl)
    test -f "$JARVIS_MCP_INSTALL_SPEC"
    COMPONENT_DOWNLOAD_DIR="$HOME/.local/share/clio-relay/component-wheels/clio-kit"
    test -d "$COMPONENT_DOWNLOAD_DIR"
    COMPONENT_STAGING="$(mktemp "$BOOTSTRAP_TRANSACTION_ROOT/downloads/clio-kit.XXXXXX.whl")"
    cp "$JARVIS_MCP_INSTALL_SPEC" "$COMPONENT_STAGING"
    JARVIS_MCP_ARTIFACT_PATH="$COMPONENT_DOWNLOAD_DIR/$(basename "$JARVIS_MCP_INSTALL_SPEC")"
    mv "$COMPONENT_STAGING" "$JARVIS_MCP_ARTIFACT_PATH"
    JARVIS_MCP_INSTALL_TARGET="$JARVIS_MCP_ARTIFACT_PATH"
    JARVIS_MCP_REQUESTED_SOURCE="wheel"
    ;;
  *)
    echo "clio-kit source must be the pinned URL, an exact version, or a local wheel" >&2
    exit 1
    ;;
esac
echo "$JARVIS_MCP_ARTIFACT_SHA256 *$JARVIS_MCP_ARTIFACT_PATH" | \
  sha256sum --check --strict -
deactivate
bootstrap_mkdir_owned_if_absent "$HOME/.local/share/clio-relay/uv-tools" uv_tools
bootstrap_mkdir_owned_if_absent "$HOME/.local/share/clio-relay/uv-bin" uv_bin
uv tool install --force --python 3.12 --no-config \\
  --default-index https://pypi.org/simple "$JARVIS_MCP_INSTALL_TARGET"
JARVIS_MCP_UV_EXECUTABLE="$(command -v uv)"
test -x "$JARVIS_MCP_UV_EXECUTABLE"
JARVIS_MCP_EXECUTABLE="$(uv tool dir --bin --no-config)/clio-kit"
test -x "$JARVIS_MCP_EXECUTABLE"
JARVIS_MCP_PROVIDER_PYTHON="$UV_TOOL_DIR/clio-kit/bin/python"
test -x "$JARVIS_MCP_PROVIDER_PYTHON"
JARVIS_MCP_INSTALLED_VERSION="$("$JARVIS_MCP_PROVIDER_PYTHON" -c \
  'from importlib.metadata import version; print(version("clio-kit"))')"
if [ -n "$JARVIS_MCP_VERSION" ] && \
   [ "$JARVIS_MCP_INSTALLED_VERSION" != "$JARVIS_MCP_VERSION" ]; then
  echo "installed clio-kit tool version does not match the release pin" >&2
  exit 1
fi
JARVIS_MCP_VERSION="$JARVIS_MCP_INSTALLED_VERSION"
"$JARVIS_MCP_EXECUTABLE" --help >/dev/null

DEST="$BOOTSTRAP_GENERATION/source"
SOURCE_ARCHIVE={rendered_source_archive}
SOURCE_ARCHIVE_SHA256={rendered_source_archive_sha256}
if [ -n "$SOURCE_ARCHIVE_SHA256" ]; then
  echo "$SOURCE_ARCHIVE_SHA256 *$SOURCE_ARCHIVE" | sha256sum --check --strict -
fi
bootstrap_safe_extract "$JARVIS_VENV/bin/python" "$SOURCE_ARCHIVE" "$DEST"
if [ "$ACTIVATION_STAGING_MODE" != "1" ]; then
  bootstrap_journal_action symlink-owned "$BOOTSTRAP_TRANSACTION_JOURNAL" relay_source \
    "$DEST"
fi
# clio-relay#257: staging mode never creates it here -- promoted with
# `current` at the fenced full-activation-reconcile boundary below.
RELAY_INSTALL_SPEC={rendered_relay_install_spec}
RELAY_ARTIFACT_SHA256={rendered_relay_artifact_sha256}
RELAY_INSTALL_TARGET="$RELAY_INSTALL_SPEC"
RELAY_ARTIFACT_PATH=""
case "$RELAY_INSTALL_SPEC" in
  clio-relay==*)
    DOWNLOAD_DIR="$DEST/downloaded-wheels"
    rm -rf "$DOWNLOAD_DIR"
    mkdir -p "$DOWNLOAD_DIR"
    "$JARVIS_VENV/bin/python" -m pip download --isolated \
      --disable-pip-version-check --no-cache-dir \
      --index-url https://pypi.org/simple --no-deps --only-binary=:all: \
      --dest "$DOWNLOAD_DIR" "$RELAY_INSTALL_SPEC"
    mapfile -t RELAY_WHEELS < <(
      find "$DOWNLOAD_DIR" -maxdepth 1 -type f -name 'clio_relay-*.whl' -print
    )
    if [ "${{#RELAY_WHEELS[@]}}" -ne 1 ]; then
      echo "expected exactly one downloaded clio-relay wheel" >&2
      exit 1
    fi
    RELAY_ARTIFACT_PATH="${{RELAY_WHEELS[0]}}"
    BOOTSTRAP_RELAY_DOWNLOAD_COUNT=1
    RELAY_INSTALL_TARGET="$RELAY_ARTIFACT_PATH"
    if [ -z "$RELAY_ARTIFACT_SHA256" ]; then
      RELAY_VERSION="${{RELAY_INSTALL_SPEC#clio-relay==}}"
      RELAY_ARTIFACT_SHA256="$(
        "$JARVIS_VENV/bin/python" - "$RELAY_VERSION" "$(basename "$RELAY_ARTIFACT_PATH")" \
          <<'__CLIO_RELAY_PYPI_DIGEST__'
import json
import re
import sys
from urllib.parse import quote
from urllib.request import urlopen

version, filename = sys.argv[1:]
with urlopen(
    f"https://pypi.org/pypi/clio-relay/{{quote(version, safe='')}}/json",
    timeout=30,
) as response:
    content = response.read(4 * 1024 * 1024 + 1)
if len(content) > 4 * 1024 * 1024:
    raise SystemExit("PyPI clio-relay metadata exceeds the bounded response size")
document = json.loads(content)
matches = [
    item
    for item in document.get("urls", [])
    if item.get("filename") == filename and item.get("packagetype") == "bdist_wheel"
]
if len(matches) != 1:
    raise SystemExit("PyPI did not return one exact clio-relay wheel identity")
digest = matches[0].get("digests", {{}}).get("sha256")
if not isinstance(digest, str) or re.fullmatch(r"[0-9a-f]{{64}}", digest) is None:
    raise SystemExit("PyPI clio-relay wheel identity omitted a valid SHA-256")
print(digest)
__CLIO_RELAY_PYPI_DIGEST__
      )"
    fi
    ;;
  *.whl)
    RELAY_ARTIFACT_PATH="$RELAY_INSTALL_SPEC"
    ;;
esac
if [ -n "$RELAY_ARTIFACT_PATH" ]; then
  test -n "$RELAY_ARTIFACT_SHA256"
  echo "$RELAY_ARTIFACT_SHA256 *$RELAY_ARTIFACT_PATH" | sha256sum --check --strict -
fi
uv tool install --force --python 3.12 --no-config \\
  --default-index https://pypi.org/simple \\
  --with "$JARVIS_CD_WHEEL" "$RELAY_INSTALL_TARGET"
RELAY_UV_EXECUTABLE="$(command -v uv)"
test -x "$RELAY_UV_EXECUTABLE"
RELAY_EXECUTABLE="$(uv tool dir --bin --no-config)/clio-relay"
test -x "$RELAY_EXECUTABLE"
RELAY_PROVIDER_PYTHON="$UV_TOOL_DIR/clio-relay/bin/python"
test -x "$RELAY_PROVIDER_PYTHON"
uv pip install --python "$JARVIS_VENV/bin/python" \\
  --default-index https://pypi.org/simple \\
  --refresh-package clio-relay "$RELAY_INSTALL_TARGET"
JARVIS_PACKAGE_PROBE='import clio_relay, jarvis_cd; '
JARVIS_PACKAGE_PROBE+='import clio_relay.bounded_command.pkg; '
JARVIS_PACKAGE_PROBE+='import clio_relay.mcp_call.pkg; '
JARVIS_PACKAGE_PROBE+='import clio_relay.remote_agent.pkg'
"$RELAY_PROVIDER_PYTHON" -c "$JARVIS_PACKAGE_PROBE"
"$JARVIS_VENV/bin/python" -c "$JARVIS_PACKAGE_PROBE"
verify_jarvis_cd_distribution() {{
  local interpreter="$1"
  "$interpreter" - \\
    "$JARVIS_CD_WHEEL" \\
    "$JARVIS_CD_WHEEL_SHA256" \\
    "$JARVIS_CD_VERSION" \\
    <<'__CLIO_RELAY_NATIVE_JARVIS_PROBE__'
import hashlib
import sys
from importlib.metadata import distribution
from pathlib import Path

"""
