"""Rendered-script fragment: JARVIS state classification and the fresh-install desired-
fingerprint computation.

Split from bootstrap.py (clio-relay#255) -- one sequential fragment of the Linux cluster
bootstrap's rendered shell script. Pure string assembly, called only from bootstrap.py's
own renderer; not independently monkeypatched.
"""

from __future__ import annotations

import shlex

from clio_relay.bootstrap_constants import UV_LINUX_AMD64_EXECUTABLE_SHA256


def script_jarvis_state(
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
    """
    Render: JARVIS state classification and the fresh-install desired-fingerprint
    computation.
    """
    return f"""root = Path(os.environ["JARVIS_STATE_ROOT"])
try:
    root_details = root.lstat()
except FileNotFoundError:
    root_details = None
if root_details is not None and not stat.S_ISDIR(root_details.st_mode):
    raise SystemExit("JARVIS state root must be one real directory")
paths = [
    (Path(os.environ["JARVIS_CONFIG_FILE"]), 1024 * 1024),
    (Path(os.environ["JARVIS_REPOS_FILE"]), 4 * 1024 * 1024),
    (Path(os.environ["JARVIS_GRAPH_FILE"]), 64 * 1024 * 1024),
]
identities = []
count = 0
for path, maximum in paths:
    try:
        details = path.lstat()
    except FileNotFoundError:
        continue
    if not stat.S_ISREG(details.st_mode) or not 0 < details.st_size <= maximum:
        raise SystemExit(f"JARVIS state is not one bounded regular file: {{path}}")
    identities.append((details.st_dev, details.st_ino))
    count += 1
if len(set(identities)) != len(identities):
    raise SystemExit("JARVIS state files must not share one file identity")
print(count)
__CLIO_RELAY_JARVIS_STATE_CLASSIFY__
)"
if [ "$JARVIS_EXISTING_FILE_COUNT" -ne 0 ] && [ "$JARVIS_EXISTING_FILE_COUNT" -ne 3 ]; then
  echo "JARVIS state is partially initialized; refusing bootstrap mutation" >&2
  exit 1
fi
BOOTSTRAP_JARVIS_CONFIG_SHA256_BEFORE=""
BOOTSTRAP_JARVIS_REPOS_SHA256_BEFORE=""
BOOTSTRAP_JARVIS_GRAPH_SHA256_BEFORE=""
if [ "$JARVIS_EXISTING_FILE_COUNT" -eq 3 ]; then
  command -v timeout >/dev/null 2>&1 || {{
    echo "timeout is required for bounded candidate staging" >&2
    exit 1
  }}
  BOOTSTRAP_JARVIS_CONFIG_SHA256_BEFORE="$(sha256sum "$JARVIS_CONFIG_FILE" | awk '{{print $1}}')"
  BOOTSTRAP_JARVIS_REPOS_SHA256_BEFORE="$(sha256sum "$JARVIS_REPOS_FILE" | awk '{{print $1}}')"
  BOOTSTRAP_JARVIS_GRAPH_SHA256_BEFORE="$(sha256sum "$JARVIS_GRAPH_FILE" | awk '{{print $1}}')"
fi
export BOOTSTRAP_JARVIS_CONFIG_SHA256_BEFORE
export BOOTSTRAP_JARVIS_REPOS_SHA256_BEFORE
export BOOTSTRAP_JARVIS_GRAPH_SHA256_BEFORE
if [ "$JARVIS_EXISTING_FILE_COUNT" -eq 3 ] && \
   ! [ -x "$HOME/.local/share/clio-relay/jarvis-venv/bin/python" ]; then
  echo "existing JARVIS state has no verifiable relay-managed interpreter" >&2
  exit 1
fi
if [ "$JARVIS_EXISTING_FILE_COUNT" -eq 3 ]; then
  export JARVIS_CONFIG_FILE
  "$HOME/.local/share/clio-relay/jarvis-venv/bin/python" -I - <<'__CLIO_RELAY_JARVIS_ROOT_PROBE__'
import os
from pathlib import Path

import yaml

config_path = Path(os.environ["JARVIS_CONFIG_FILE"])
value = yaml.safe_load(config_path.read_text(encoding="utf-8"))
if not isinstance(value, dict):
    raise SystemExit("JARVIS configuration must contain one mapping")
for field in ("config_dir", "private_dir", "shared_dir"):
    observed = value.get(field)
    if not isinstance(observed, str):
        raise SystemExit(f"JARVIS {{field}} is missing")
    path = Path(observed).expanduser()
    if not path.is_absolute() or not path.resolve(strict=True).is_dir():
        raise SystemExit(f"JARVIS {{field}} is not one existing absolute directory")
print("jarvis_existing_roots=verified")
__CLIO_RELAY_JARVIS_ROOT_PROBE__
fi
{relay_only_reconcile}
BOOTSTRAP_PREPARING_PARENT="$HOME/.local/share/clio-relay/preparing"
BOOTSTRAP_PREPARING_ROOT="$BOOTSTRAP_PREPARING_PARENT/active"
export BOOTSTRAP_PREPARING_PARENT BOOTSTRAP_PREPARING_ROOT
mkdir -m 0700 -p "$BOOTSTRAP_PREPARING_PARENT"
env -u PYTHONPATH -u PYTHONHOME -u LD_PRELOAD -u LD_LIBRARY_PATH \
  python3 -I -c {preparing_root_program} \
  "$BOOTSTRAP_PREPARING_PARENT" "$BOOTSTRAP_PREPARING_ROOT" prepare
bootstrap_cleanup_preparing_root() {{
  env -u PYTHONPATH -u PYTHONHOME -u LD_PRELOAD -u LD_LIBRARY_PATH \
    python3 -I -c {preparing_root_program} \
    "$BOOTSTRAP_PREPARING_PARENT" "$BOOTSTRAP_PREPARING_ROOT" cleanup
}}
trap bootstrap_cleanup_preparing_root EXIT
BOOTSTRAP_CANDIDATE_PYTHON_ROOT="$BOOTSTRAP_PREPARING_ROOT/candidate-python"
BOOTSTRAP_CANDIDATE_PACKAGE="$BOOTSTRAP_CANDIDATE_PYTHON_ROOT/clio_relay"
if [ -L "$BOOTSTRAP_CANDIDATE_PYTHON_ROOT" ] || \
   [ -L "$BOOTSTRAP_CANDIDATE_PACKAGE" ]; then
  echo "bootstrap candidate package root must not be a symbolic link" >&2
  exit 1
fi
mkdir -m 0700 -p "$BOOTSTRAP_CANDIDATE_PACKAGE"
python3 -I - "$BOOTSTRAP_CANDIDATE_PACKAGE" <<'__CLIO_RELAY_CANDIDATE_PACKAGE__'
import base64
import hashlib
import json
import os
import sys
from pathlib import Path

destination = Path(sys.argv[1])
encoded_sources = json.loads(r'''{rendered_candidate_package_sources}''')
if not isinstance(encoded_sources, dict):
    raise SystemExit("bootstrap candidate source manifest is invalid")
sources = {{
    name: base64.b64decode(encoded, validate=True)
    for name, encoded in encoded_sources.items()
}}
for name, payload in sources.items():
    path = destination / name
    if path.is_symlink():
        raise SystemExit(f"bootstrap candidate source must not be a symbolic link: {{name}}")
    try:
        observed = path.read_bytes()
    except FileNotFoundError:
        with path.open("xb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.chmod(path, 0o600)
        observed = payload
    if observed != payload:
        raise SystemExit(f"bootstrap candidate source identity changed: {{name}}")
    print(f"bootstrap_candidate_source={{name}}:{{hashlib.sha256(observed).hexdigest()}}")
__CLIO_RELAY_CANDIDATE_PACKAGE__
BOOTSTRAP_CANDIDATE_RECONCILE="$BOOTSTRAP_CANDIDATE_PACKAGE/bootstrap_reconcile.py"
BOOTSTRAP_CANDIDATE_PROVIDER_BUILD_INFO="$BOOTSTRAP_CANDIDATE_PACKAGE/bootstrap_provider_build_info.py"
BOOTSTRAP_CANDIDATE_PROCESS_CONTAINMENT="$BOOTSTRAP_CANDIDATE_PACKAGE/process_containment.py"
echo "{candidate_reconcile_sha256} *$BOOTSTRAP_CANDIDATE_RECONCILE" | \
  sha256sum --check --strict -
echo "{candidate_provider_build_info_sha256} *$BOOTSTRAP_CANDIDATE_PROVIDER_BUILD_INFO" | \
  sha256sum --check --strict -
echo "{candidate_bounded_process_sha256} *$BOOTSTRAP_CANDIDATE_PACKAGE/bounded_process.py" | \
  sha256sum --check --strict -
echo "{candidate_errors_sha256} *$BOOTSTRAP_CANDIDATE_PACKAGE/errors.py" | \
  sha256sum --check --strict -
echo "{candidate_process_containment_sha256} *$BOOTSTRAP_CANDIDATE_PROCESS_CONTAINMENT" | \
  sha256sum --check --strict -
echo "{candidate_safe_archive_sha256} *$BOOTSTRAP_CANDIDATE_PACKAGE/safe_archive.py" | \
  sha256sum --check --strict -
export BOOTSTRAP_CANDIDATE_PYTHON_ROOT BOOTSTRAP_CANDIDATE_RECONCILE
bootstrap_safe_extract() {{
  local provider="$1"
  local archive="$2"
  local destination="$3"
  bootstrap_safe_extract_provider() {{
    if [ "${{BOOTSTRAP_CANDIDATE_PROVIDER_READY:-0}}" = "1" ] && \
       [ "$provider" = "${{BOOTSTRAP_PLAN_PROVIDER:-}}" ]; then
      bootstrap_provider_exec "$@"
    else
      "$provider" -I "$@"
    fi
  }}
  bootstrap_safe_extract_provider - "$BOOTSTRAP_CANDIDATE_PYTHON_ROOT" \
    "$archive" "$destination" \
      <<'__CLIO_RELAY_SAFE_EXTRACT__'
import json
import sys
from pathlib import Path

candidate_root, archive_value, destination_value = sys.argv[1:]
sys.path.insert(0, candidate_root)
from clio_relay.safe_archive import safe_extract_tar

receipt = safe_extract_tar(Path(archive_value), Path(destination_value))
print(
    "bootstrap_archive_extraction="
    + json.dumps(
        {{
            "archive_bytes": receipt.archive_bytes,
            "destination": str(receipt.destination),
            "directory_count": receipt.directory_count,
            "extracted_bytes": receipt.extracted_bytes,
            "member_count": receipt.member_count,
            "regular_file_count": receipt.regular_file_count,
        }},
        sort_keys=True,
        separators=(",", ":"),
    )
)
__CLIO_RELAY_SAFE_EXTRACT__
}}
BOOTSTRAP_PLAN_MODE="full"
BOOTSTRAP_PLAN_JSON=""
BOOTSTRAP_RECOVERY_PROVIDER=""
BOOTSTRAP_CANDIDATE_PROVIDER_READY=0
if [ -x "$BOOTSTRAP_CURRENT_PROVIDER" ]; then
  BOOTSTRAP_RECOVERY_PROVIDER="$BOOTSTRAP_CURRENT_PROVIDER"
elif [ -x "$HOME/.local/share/clio-relay/jarvis-venv/bin/python" ]; then
  BOOTSTRAP_RECOVERY_PROVIDER="$HOME/.local/share/clio-relay/jarvis-venv/bin/python"
fi
export BOOTSTRAP_RECOVERY_PROVIDER BOOTSTRAP_CANDIDATE_PROVIDER_READY
export BOOTSTRAP_CANDIDATE_RECONCILE
if [ "$BOOTSTRAP_RECOVERY_REQUIRED" = "1" ]; then
  if [ -z "$BOOTSTRAP_RECOVERY_PROVIDER" ]; then
    echo "bootstrap recovery has no trusted installed Python provider" >&2
    exit 1
  fi
  env -u PYTHONPATH -u PYTHONHOME -u LD_PRELOAD -u LD_LIBRARY_PATH \
    "$BOOTSTRAP_RECOVERY_PROVIDER" -I "$BOOTSTRAP_CANDIDATE_PROVIDER_BUILD_INFO" \
      "$BOOTSTRAP_CANDIDATE_PACKAGE"
  bootstrap_recover_previous_transaction
  exec 9>&-
  unset CLIO_RELAY_BOOTSTRAP_LOCK_FD
  bootstrap_cleanup_preparing_root
  trap - EXIT
  exec bash "$0"
fi
if [ "$JARVIS_EXISTING_FILE_COUNT" -eq 3 ]; then
  SOURCE_ARCHIVE={rendered_source_archive}
  SOURCE_ARCHIVE_SHA256={rendered_source_archive_sha256}
  if [ -z "$SOURCE_ARCHIVE_SHA256" ]; then
    echo "retained-state reconcile requires a verified source archive digest" >&2
    exit 1
  fi
  echo "$SOURCE_ARCHIVE_SHA256 *$SOURCE_ARCHIVE" | sha256sum --check --strict -
  BOOTSTRAP_CANDIDATE_SOURCE_ROOT="$BOOTSTRAP_PREPARING_ROOT/source"
  bootstrap_safe_extract python3 "$SOURCE_ARCHIVE" "$BOOTSTRAP_CANDIDATE_SOURCE_ROOT"

  BOOTSTRAP_CANDIDATE_INSTALL_SPEC={rendered_candidate_relay_install_spec}
  BOOTSTRAP_CANDIDATE_ARTIFACT_SHA256={rendered_relay_artifact_sha256}
  if [ -z "$BOOTSTRAP_CANDIDATE_ARTIFACT_SHA256" ]; then
    echo "retained-state reconcile requires an exact relay wheel SHA-256" >&2
    exit 1
  fi
  BOOTSTRAP_CANDIDATE_ARTIFACT=""
  case "$BOOTSTRAP_CANDIDATE_INSTALL_SPEC" in
    '$DEST/'*)
      BOOTSTRAP_CANDIDATE_ARTIFACT="$(
        python3 -I - "$BOOTSTRAP_CANDIDATE_INSTALL_SPEC" \
          "$BOOTSTRAP_CANDIDATE_SOURCE_ROOT" <<'__CLIO_RELAY_CANDIDATE_WHEEL_PATH__'
import sys
from pathlib import Path

specification, source_value = sys.argv[1:]
if not specification.startswith("$DEST/"):
    raise SystemExit("transported relay wheel did not use the archive destination")
source = Path(source_value).resolve(strict=True)
lexical_candidate = source / specification.removeprefix("$DEST/")
candidate_details = lexical_candidate.lstat()
if lexical_candidate.is_symlink() or not lexical_candidate.is_file():
    raise SystemExit("transported relay wheel is not one regular file")
candidate = lexical_candidate.resolve(strict=True)
if candidate == source or not candidate.is_relative_to(source):
    raise SystemExit("transported relay wheel escaped the verified source archive")
observed = lexical_candidate.lstat()
identity = lambda value: (
    value.st_dev,
    value.st_ino,
    value.st_mode,
    value.st_size,
    value.st_mtime_ns,
    value.st_ctime_ns,
)
if identity(observed) != identity(candidate_details) or candidate.suffix != ".whl":
    raise SystemExit("transported relay wheel is not one regular wheel file")
print(candidate)
__CLIO_RELAY_CANDIDATE_WHEEL_PATH__
      )"
      ;;
    clio-relay==*)
      BOOTSTRAP_CANDIDATE_ARTIFACT="$(
        timeout --signal=TERM --kill-after=5s 180 \
          python3 -I - "$BOOTSTRAP_CANDIDATE_INSTALL_SPEC" \
          "$BOOTSTRAP_CANDIDATE_ARTIFACT_SHA256" \
          "$BOOTSTRAP_PREPARING_ROOT" <<'__CLIO_RELAY_CANDIDATE_PYPI_WHEEL__'
import hashlib
import json
import sys
from pathlib import Path, PurePosixPath
from urllib.parse import quote, unquote, urlsplit
from urllib.request import urlopen

specification, expected_sha256, destination_root_value = sys.argv[1:]
version = specification.removeprefix("clio-relay==")
if not version or specification != f"clio-relay=={{version}}":
    raise SystemExit("candidate PyPI install spec is not exact")
with urlopen(
    f"https://pypi.org/pypi/clio-relay/{{quote(version, safe='')}}/json",
    timeout=30,
) as response:
    metadata = response.read(4 * 1024 * 1024 + 1)
if len(metadata) > 4 * 1024 * 1024:
    raise SystemExit("candidate PyPI metadata exceeds its bound")
document = json.loads(metadata)
expected_filename = f"clio_relay-{{version}}-py3-none-any.whl"
matches = [
    item
    for item in document.get("urls", [])
    if isinstance(item, dict)
    and item.get("filename") == expected_filename
    and item.get("packagetype") == "bdist_wheel"
    and item.get("digests", {{}}).get("sha256") == expected_sha256
]
if len(matches) != 1:
    raise SystemExit("PyPI did not return the exact digest-pinned relay wheel")
filename = matches[0].get("filename")
if (
    not isinstance(filename, str)
    or not filename.endswith(".whl")
    or Path(filename).name != filename
    or any(character in filename for character in "\\x00\\r\\n")
):
    raise SystemExit("PyPI relay wheel filename is unsafe")
url = matches[0].get("url")
if not isinstance(url, str):
    raise SystemExit("PyPI relay wheel URL is missing")
parsed = urlsplit(url)
if parsed.scheme != "https" or not parsed.hostname or not parsed.hostname.endswith(
    ".pythonhosted.org"
):
    raise SystemExit("PyPI relay wheel URL has an unsupported origin")
if PurePosixPath(unquote(parsed.path)).name != filename:
    raise SystemExit("PyPI relay wheel URL does not preserve its verified filename")
destination_root = Path(destination_root_value)
if (
    not destination_root.is_absolute()
    or destination_root.is_symlink()
    or not destination_root.is_dir()
):
    raise SystemExit("candidate relay wheel destination root is unsafe")
destination_root = destination_root.resolve(strict=True)
destination = destination_root / filename
if destination.parent != destination_root:
    raise SystemExit("candidate relay wheel destination escaped its private root")
digest = hashlib.sha256()
size = 0
with urlopen(url, timeout=60) as response, destination.open("xb") as stream:
    while chunk := response.read(1024 * 1024):
        size += len(chunk)
        if size > 256 * 1024 * 1024:
            raise SystemExit("candidate relay wheel exceeds its bound")
        digest.update(chunk)
        stream.write(chunk)
if size < 1 or digest.hexdigest() != expected_sha256:
    destination.unlink(missing_ok=True)
    raise SystemExit("downloaded candidate relay wheel did not match its digest")
print(destination)
__CLIO_RELAY_CANDIDATE_PYPI_WHEEL__
      )"
      ;;
    *)
      echo "retained-state reconcile supports only an exact released relay wheel" >&2
      exit 1
      ;;
  esac
  echo "$BOOTSTRAP_CANDIDATE_ARTIFACT_SHA256 *$BOOTSTRAP_CANDIDATE_ARTIFACT" | \
    sha256sum --check --strict -

  BOOTSTRAP_PINNED_UV_SOURCE="$HOME/.local/bin/uv"
  BOOTSTRAP_PINNED_UV="$(
    env -u PYTHONPATH -u PYTHONHOME -u LD_PRELOAD -u LD_LIBRARY_PATH \
      python3 -I -c {pinned_uv_copy_program} \
      "$BOOTSTRAP_PINNED_UV_SOURCE" "$BOOTSTRAP_PREPARING_ROOT" \
      {UV_LINUX_AMD64_EXECUTABLE_SHA256}
  )"
  BOOTSTRAP_CANDIDATE_TOOL_DIR="$BOOTSTRAP_PREPARING_ROOT/uv-tools"
  BOOTSTRAP_CANDIDATE_BIN_DIR="$BOOTSTRAP_PREPARING_ROOT/uv-bin"
  BOOTSTRAP_CANDIDATE_CACHE_DIR="$BOOTSTRAP_PREPARING_ROOT/uv-cache"
  BOOTSTRAP_CANDIDATE_PYTHON_INSTALL_DIR="$HOME/.local/share/clio-relay/uv-python"
  mkdir -m 0700 "$BOOTSTRAP_CANDIDATE_CACHE_DIR"
  BOOTSTRAP_CANDIDATE_RELAY="$BOOTSTRAP_CANDIDATE_BIN_DIR/clio-relay"
  BOOTSTRAP_PLAN_PROVIDER="$BOOTSTRAP_CANDIDATE_TOOL_DIR/clio-relay/bin/python"
  export BOOTSTRAP_PLAN_PROVIDER BOOTSTRAP_CANDIDATE_RECONCILE
  export BOOTSTRAP_CANDIDATE_SOURCE_ROOT BOOTSTRAP_CANDIDATE_ARTIFACT
  export BOOTSTRAP_CANDIDATE_TOOL_DIR BOOTSTRAP_CANDIDATE_BIN_DIR
  export BOOTSTRAP_CANDIDATE_CACHE_DIR BOOTSTRAP_CANDIDATE_PYTHON_INSTALL_DIR
  export BOOTSTRAP_CANDIDATE_RELAY BOOTSTRAP_PINNED_UV SOURCE_ARCHIVE_SHA256
  BOOTSTRAP_PLAN_STARTED_NS="$(python3 -c 'import time; print(time.monotonic_ns())')"
  BOOTSTRAP_PLAN_CAPTURE="$(
    python3 -I -c {candidate_uv_install_program} \
      install-verify-and-exec \
      "$BOOTSTRAP_PINNED_UV" {UV_LINUX_AMD64_EXECUTABLE_SHA256} \
      "$BOOTSTRAP_CANDIDATE_ARTIFACT" "$BOOTSTRAP_CANDIDATE_ARTIFACT_SHA256" \
      "$BOOTSTRAP_CANDIDATE_TOOL_DIR" "$BOOTSTRAP_CANDIDATE_BIN_DIR" \
      "$BOOTSTRAP_CANDIDATE_CACHE_DIR" \
      "$BOOTSTRAP_CANDIDATE_PYTHON_INSTALL_DIR" -I - \
      <<'__CLIO_RELAY_RECONCILE_PLAN__'
import json
import os
from pathlib import Path

from clio_relay.bootstrap_reconcile import (
    BootstrapDesiredState,
    plan_bootstrap_reconcile,
    prove_bootstrap_replacement_provider,
)

desired_payload = json.loads(os.environ["BOOTSTRAP_DESIRED_STATE"])
desired_payload["agent_npm_package"] = os.environ["AGENT_NPM_PACKAGE"] or None
desired_payload["agent_npm_bin"] = os.environ["AGENT_NPM_BIN"] or None
desired = BootstrapDesiredState.model_validate(desired_payload)
evidence = prove_bootstrap_replacement_provider(
    desired,
    uv_executable=Path(os.environ["BOOTSTRAP_PINNED_UV"]),
    tool_executable=Path(os.environ["BOOTSTRAP_CANDIDATE_RELAY"]),
    source_artifact=Path(os.environ["BOOTSTRAP_CANDIDATE_ARTIFACT"]),
    tool_directory=Path(os.environ["BOOTSTRAP_CANDIDATE_TOOL_DIR"]),
    tool_bin_directory=Path(os.environ["BOOTSTRAP_CANDIDATE_BIN_DIR"]),
    preparing_root=Path(os.environ["BOOTSTRAP_PREPARING_ROOT"]),
    extracted_source_root=Path(os.environ["BOOTSTRAP_CANDIDATE_SOURCE_ROOT"]),
    source_archive_sha256=os.environ["SOURCE_ARCHIVE_SHA256"],
    expected_provider_interpreter_sha256=os.environ["BOOTSTRAP_PLAN_PROVIDER_SHA256"],
)
plan = plan_bootstrap_reconcile(desired, replacement_provider=evidence)
print("bootstrap_candidate_provider_sha256=" + os.environ["BOOTSTRAP_PLAN_PROVIDER_SHA256"])
print(json.dumps(plan.model_dump(mode="json"), sort_keys=True, separators=(",", ":")))
__CLIO_RELAY_RECONCILE_PLAN__
  )"
  BOOTSTRAP_PLAN_JSON=""
  BOOTSTRAP_CANDIDATE_PROVIDER_SHA256=""
  while IFS= read -r bootstrap_plan_line; do
    case "$bootstrap_plan_line" in
      bootstrap_candidate_provider_sha256=*)
        BOOTSTRAP_CANDIDATE_PROVIDER_SHA256="${{bootstrap_plan_line#*=}}"
        ;;
      '{{'*'}}')
        if [ -n "$BOOTSTRAP_PLAN_JSON" ]; then
          echo "candidate planner returned multiple plan objects" >&2
          exit 1
        fi
        BOOTSTRAP_PLAN_JSON="$bootstrap_plan_line"
        ;;
      *)
        echo "candidate planner returned unrecognized output" >&2
        exit 1
        ;;
    esac
  done <<< "$BOOTSTRAP_PLAN_CAPTURE"
  if [ "${{#BOOTSTRAP_CANDIDATE_PROVIDER_SHA256}}" -ne 64 ]; then
    echo "candidate planner omitted its pinned provider digest" >&2
    exit 1
  fi
  case "$BOOTSTRAP_CANDIDATE_PROVIDER_SHA256" in
    *[!0-9a-f]*) echo "candidate planner provider digest is invalid" >&2; exit 1 ;;
  esac
  BOOTSTRAP_CANDIDATE_PROVIDER_READY=1
  export BOOTSTRAP_CANDIDATE_PROVIDER_READY BOOTSTRAP_CANDIDATE_PROVIDER_SHA256
  BOOTSTRAP_PLAN_COMPLETED_NS="$(python3 -c 'import time; print(time.monotonic_ns())')"
  export BOOTSTRAP_PLAN_JSON
  BOOTSTRAP_PLAN_MODE="$(
    python3 -c 'import json,os; print(json.loads(os.environ["BOOTSTRAP_PLAN_JSON"])["mode"])'
  )"
fi
export BOOTSTRAP_PLAN_MODE BOOTSTRAP_PLAN_JSON BOOTSTRAP_PLAN_STARTED_NS \
  BOOTSTRAP_PLAN_COMPLETED_NS
if [ "$BOOTSTRAP_PLAN_MODE" = "repair" ]; then
  bootstrap_reuse_repair
  exit 0
fi
if [ "$BOOTSTRAP_PLAN_MODE" = "relay-only" ] || \
   [ "$BOOTSTRAP_PLAN_MODE" = "component-upgrade" ]; then
  bootstrap_relay_only_reconcile
  exit 0
fi
BOOTSTRAP_INVOCATION_ID={shlex.quote(invocation_id)}
JARVIS_STAGING_MODE=0
JARVIS_VENV_LIVE_PATH="$HOME/.local/share/clio-relay/jarvis-venv"
JARVIS_VENV_STAGED_PATH="$JARVIS_VENV_LIVE_PATH.staging-$BOOTSTRAP_INVOCATION_ID"
if [ "$BOOTSTRAP_PLAN_MODE" = "full" ] && \
   {{ [ "$JARVIS_EXISTING_FILE_COUNT" -eq 3 ] || [ -e "$JARVIS_VENV_LIVE_PATH" ]; }}; then
  # clio-relay#254: a managed jarvis-venv already exists -- stage the
  # replacement at a path this transaction owns instead of refusing. The
  # live environment is never cleared directly: it is promoted-in with one
  # atomic pathname exchange at the same fenced boundary that activates the
  # rest of the generation (see JARVIS_VENV below and the migration_started
  # promotion call), and only retired -- never deleted -- once that exchange
  # is durable.
  if [ -L "$JARVIS_VENV_LIVE_PATH" ] || [ ! -d "$JARVIS_VENV_LIVE_PATH" ]; then
    printf '%s\\n' "bootstrap_reconcile_plan=$BOOTSTRAP_PLAN_JSON" >&2
    echo "existing jarvis execution environment is not one owned directory" >&2
    exit 1
  fi
  JARVIS_STAGING_MODE=1
fi
# clio-relay#257: current already resolving means the host is populated.
ACTIVATION_STAGING_MODE=0
if [ -e "$HOME/.local/share/clio-relay/current" ] || \
   [ -L "$HOME/.local/share/clio-relay/current" ]; then
  ACTIVATION_STAGING_MODE=1
fi
export ACTIVATION_STAGING_MODE
if [ "$BOOTSTRAP_PLAN_MODE" = "full" ] && \
   [ "$JARVIS_EXISTING_FILE_COUNT" -eq 0 ] && \
   [ -z "$JARVIS_RESOURCE_GRAPH_PROFILE" ]; then
  echo "fresh bootstrap requires an operator-selected JARVIS resource graph profile" >&2
  exit 1
fi
BOOTSTRAP_DESIRED_FINGERPRINT="$(
  python3 - <<'__CLIO_RELAY_FRESH_DESIRED_FINGERPRINT__'
import hashlib
import json
import os

"""
