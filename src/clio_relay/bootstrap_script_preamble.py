"""Rendered-script fragment: shell preamble, environment sanitizing, the artifact-fetch
function, and the current/staged generation-identity shell functions.

Split from bootstrap.py (clio-relay#255) -- one sequential fragment of the Linux cluster
bootstrap's rendered shell script. Pure string assembly, called only from bootstrap.py's
own renderer; not independently monkeypatched.
"""

from __future__ import annotations

import shlex

from clio_relay.bootstrap_receipt_classifier_source import _BOOTSTRAP_RECEIPT_CLASSIFIER_SOURCE


def script_preamble(
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
    Render: shell preamble, environment sanitizing, the artifact-fetch function, and the
    current/staged generation-identity shell functions.
    """
    return f"""umask 077
export PATH="$HOME/.local/bin:$PATH"
while IFS= read -r variable_name; do
  case "$variable_name" in
    LD_*|PYTHON*|BASH_ENV|ENV) unset "$variable_name" ;;
  esac
done < <(compgen -e)
export UV_TOOL_DIR="$HOME/.local/share/clio-relay/uv-tools"
export UV_TOOL_BIN_DIR="$HOME/.local/share/clio-relay/uv-bin"
export UV_PYTHON_INSTALL_DIR="$HOME/.local/share/clio-relay/uv-python"
while IFS= read -r variable_name; do
  case "$variable_name" in
    UV_TOOL_DIR|UV_TOOL_BIN_DIR|UV_PYTHON_INSTALL_DIR|UV_CACHE_DIR) ;;
    UV_*|PIP_*) unset "$variable_name" ;;
  esac
done < <(compgen -e)
mkdir -p "$HOME/.local/bin" "$HOME/.local/src" "$HOME/.local/share/clio-relay"
{artifact_fetch_function}
bootstrap_active_generation_identity() {{
  local current target prefix identity
  current="$HOME/.local/share/clio-relay/current"
  if [ ! -L "$current" ]; then
    echo "bootstrap current generation pointer is not a symbolic link" >&2
    return 1
  fi
  target="$(readlink -f "$current")"
  prefix="$(readlink -f "$HOME/.local/share/clio-relay/generations")/"
  case "$target" in
    "$prefix"*) identity="${{target#"$prefix"}}" ;;
    *)
      echo "bootstrap current pointer does not name one managed generation" >&2
      return 1
      ;;
  esac
  case "$identity" in
    (*[!0-9a-f]*|'')
      echo "bootstrap current generation identity is invalid" >&2
      return 1
      ;;
  esac
  if [ "${{#identity}}" -ne 64 ]; then
    echo "bootstrap current generation identity has an invalid length" >&2
    return 1
  fi
  echo "$identity"
}}
bootstrap_active_generation_provider() {{
  local identity generations provider
  identity="$(bootstrap_active_generation_identity)" || return 1
  generations="$(readlink -f "$HOME/.local/share/clio-relay/generations")"
  provider="$generations/$identity/tools/clio-relay/bin/python"
  if [ ! -f "$provider" ] || [ ! -x "$provider" ]; then
    echo "bootstrap active generation provider is unavailable" >&2
    return 1
  fi
  echo "$provider"
}}
command -v flock >/dev/null 2>&1 || {{
  echo "flock is required to serialize clio-relay bootstrap" >&2
  exit 1
}}
if [ "${{CLIO_RELAY_BOOTSTRAP_LOCK_FD:-}}" != 9 ]; then
  python3 - "$0" <<'__CLIO_RELAY_BOOTSTRAP_LOCK_AND_REEXEC__'
import fcntl
import os
import stat
import sys
from pathlib import Path

directory = Path.home() / ".local/share/clio-relay"
directory_flags = os.O_RDONLY
for flag_name in ("O_CLOEXEC", "O_DIRECTORY", "O_NOFOLLOW"):
    directory_flags |= getattr(os, flag_name, 0)
try:
    directory_descriptor = os.open(directory, directory_flags)
except OSError as exc:
    raise SystemExit("bootstrap lock directory must be a real directory") from exc
if directory_descriptor == 9:
    replacement_descriptor = os.dup(directory_descriptor)
    os.close(directory_descriptor)
    directory_descriptor = replacement_descriptor
descriptor = None
try:
    opened_directory = os.fstat(directory_descriptor)
    linked_directory = directory.lstat()
    if (
        not stat.S_ISDIR(opened_directory.st_mode)
        or not stat.S_ISDIR(linked_directory.st_mode)
        or opened_directory.st_uid != os.getuid()
        or (opened_directory.st_dev, opened_directory.st_ino)
        != (linked_directory.st_dev, linked_directory.st_ino)
    ):
        raise SystemExit("bootstrap lock directory must be an owned real directory")
    if stat.S_IMODE(opened_directory.st_mode) != 0o700:
        os.fchmod(directory_descriptor, 0o700)
    repaired_directory = os.fstat(directory_descriptor)
    relinked_directory = directory.lstat()
    if (
        not stat.S_ISDIR(repaired_directory.st_mode)
        or not stat.S_ISDIR(relinked_directory.st_mode)
        or repaired_directory.st_uid != os.getuid()
        or stat.S_IMODE(repaired_directory.st_mode) != 0o700
        or (repaired_directory.st_dev, repaired_directory.st_ino)
        != (relinked_directory.st_dev, relinked_directory.st_ino)
    ):
        raise SystemExit("bootstrap lock directory could not be made owner-private")
    flags = os.O_RDWR | os.O_CREAT
    for flag_name in ("O_CLOEXEC", "O_NOFOLLOW"):
        flags |= getattr(os, flag_name, 0)
    descriptor = os.open("bootstrap.lock", flags, 0o600, dir_fd=directory_descriptor)
    opened = os.fstat(descriptor)
    linked = os.stat(
        "bootstrap.lock",
        dir_fd=directory_descriptor,
        follow_symlinks=False,
    )
    if (
        not stat.S_ISREG(opened.st_mode)
        or not stat.S_ISREG(linked.st_mode)
        or opened.st_nlink != 1
        or opened.st_uid != os.getuid()
        or (opened.st_dev, opened.st_ino) != (linked.st_dev, linked.st_ino)
    ):
        raise SystemExit("bootstrap lock must be one owned regular file")
    if stat.S_IMODE(opened.st_mode) != 0o600:
        os.fchmod(descriptor, 0o600)
    repaired = os.fstat(descriptor)
    relinked = os.stat(
        "bootstrap.lock",
        dir_fd=directory_descriptor,
        follow_symlinks=False,
    )
    if (
        not stat.S_ISREG(repaired.st_mode)
        or not stat.S_ISREG(relinked.st_mode)
        or repaired.st_nlink != 1
        or repaired.st_uid != os.getuid()
        or stat.S_IMODE(repaired.st_mode) != 0o600
        or (repaired.st_dev, repaired.st_ino) != (relinked.st_dev, relinked.st_ino)
    ):
        raise SystemExit("bootstrap lock could not be made owner-private")
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError as exc:
        raise SystemExit("another clio-relay bootstrap is already running") from exc
    os.dup2(descriptor, 9, inheritable=True)
finally:
    os.close(directory_descriptor)
    if descriptor is not None and descriptor != 9:
        os.close(descriptor)
environment = dict(os.environ)
environment["CLIO_RELAY_BOOTSTRAP_LOCK_FD"] = "9"
script = str(Path(sys.argv[1]).resolve(strict=True))
os.execve("/bin/bash", ["bash", script], environment)
__CLIO_RELAY_BOOTSTRAP_LOCK_AND_REEXEC__
  exit $?
fi
python3 - <<'__CLIO_RELAY_BOOTSTRAP_LOCK_VERIFY__'
import fcntl
import os
import stat
from pathlib import Path

directory = Path.home() / ".local/share/clio-relay"
directory_flags = os.O_RDONLY
for flag_name in ("O_CLOEXEC", "O_DIRECTORY", "O_NOFOLLOW"):
    directory_flags |= getattr(os, flag_name, 0)
try:
    directory_descriptor = os.open(directory, directory_flags)
except OSError as exc:
    raise SystemExit("inherited bootstrap lock directory changed") from exc
opened = os.fstat(9)
try:
    opened_directory = os.fstat(directory_descriptor)
    linked_directory = directory.lstat()
    linked = os.stat(
        "bootstrap.lock",
        dir_fd=directory_descriptor,
        follow_symlinks=False,
    )
    if (
        not stat.S_ISDIR(opened_directory.st_mode)
        or not stat.S_ISDIR(linked_directory.st_mode)
        or opened_directory.st_uid != os.getuid()
        or stat.S_IMODE(opened_directory.st_mode) != 0o700
        or (opened_directory.st_dev, opened_directory.st_ino)
        != (linked_directory.st_dev, linked_directory.st_ino)
        or not stat.S_ISREG(opened.st_mode)
        or not stat.S_ISREG(linked.st_mode)
        or opened.st_nlink != 1
        or opened.st_uid != os.getuid()
        or stat.S_IMODE(opened.st_mode) != 0o600
        or (opened.st_dev, opened.st_ino) != (linked.st_dev, linked.st_ino)
    ):
        raise SystemExit("inherited bootstrap lock identity changed")
    fcntl.flock(9, fcntl.LOCK_EX | fcntl.LOCK_NB)
finally:
    os.close(directory_descriptor)
__CLIO_RELAY_BOOTSTRAP_LOCK_VERIFY__
BOOTSTRAP_INVOCATION_STARTED_AT="$(date -u +%Y-%m-%dT%H:%M:%S.%NZ)"
BOOTSTRAP_INVOCATION_STARTED_NS="$(python3 -c 'import time; print(time.monotonic_ns())')"
read -r BOOTSTRAP_PAYLOAD_TRANSFER_COUNT BOOTSTRAP_PAYLOAD_TRANSFER_BYTES < <(
  python3 - "$0" {rendered_source_archive} <<'__CLIO_RELAY_PAYLOAD_IDENTITY__'
import os
import stat
import sys
from pathlib import Path

total = 0
for value in sys.argv[1:]:
    path = Path(value)
    before = path.lstat()
    if path.is_symlink() or not stat.S_ISREG(before.st_mode):
        raise SystemExit(f"bootstrap payload is not one regular file: {{path}}")
    total += before.st_size
    after = path.lstat()
    if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) != (
        after.st_dev,
        after.st_ino,
        after.st_size,
        after.st_mtime_ns,
    ):
        raise SystemExit(f"bootstrap payload changed during inspection: {{path}}")
print(len(sys.argv) - 1, total)
__CLIO_RELAY_PAYLOAD_IDENTITY__
)
export BOOTSTRAP_INVOCATION_STARTED_AT BOOTSTRAP_INVOCATION_STARTED_NS
export BOOTSTRAP_PAYLOAD_TRANSFER_COUNT BOOTSTRAP_PAYLOAD_TRANSFER_BYTES
AGENT_NPM_PACKAGE={rendered_agent_npm_package}
AGENT_NPM_BIN={rendered_agent_npm_bin}
JARVIS_RESOURCE_GRAPH_PROFILE={rendered_jarvis_resource_graph_profile}
ALLOW_JARVIS_RESOURCE_GRAPH_BUILD={rendered_allow_jarvis_resource_graph_build}
AGENT_BIN=""
if [ -z "$AGENT_BIN" ] && [ -n "$AGENT_NPM_BIN" ]; then
  AGENT_BIN="$HOME/.local/bin/$AGENT_NPM_BIN"
fi
BOOTSTRAP_DESIRED_STATE={rendered_desired_state}
export BOOTSTRAP_DESIRED_STATE AGENT_NPM_PACKAGE AGENT_NPM_BIN AGENT_BIN
export JARVIS_RESOURCE_GRAPH_PROFILE ALLOW_JARVIS_RESOURCE_GRAPH_BUILD
bootstrap_journal_action() {{
  python3 - "$@" <<'__CLIO_RELAY_BOOTSTRAP_JOURNAL_ACTION__'
import base64

source = base64.b64decode(
    "{rendered_bootstrap_journal_source}",
    validate=True,
)
namespace = {{"__name__": "__main__", "__file__": "bootstrap_journal.py"}}
exec(compile(source, "bootstrap_journal.py", "exec"), namespace)
__CLIO_RELAY_BOOTSTRAP_JOURNAL_ACTION__
}}
bootstrap_path_set_identity() {{
  python3 - "$@" <<'__CLIO_RELAY_BOOTSTRAP_PATH_SET_IDENTITY__'
import hashlib
import json
import os
import stat
import sys
from pathlib import Path

evidence = []
for value in sys.argv[1:]:
    path = Path(value)
    details = path.lstat()
    if stat.S_ISREG(details.st_mode):
        digest = hashlib.sha256()
        with path.open("rb") as stream:
            while chunk := stream.read(1024 * 1024):
                digest.update(chunk)
        identity = {{"kind": "file", "sha256": digest.hexdigest(), "size": details.st_size}}
    elif stat.S_ISLNK(details.st_mode):
        identity = {{"kind": "symlink", "target": os.readlink(path)}}
    elif stat.S_ISDIR(details.st_mode):
        identity = {{
            "kind": "directory",
            "device": details.st_dev,
            "inode": details.st_ino,
        }}
    else:
        raise SystemExit(f"bootstrap phase path has an unsupported type: {{path}}")
    evidence.append({{"path": str(path), "identity": identity}})
payload = json.dumps(evidence, sort_keys=True, separators=(",", ":")).encode("utf-8")
print(hashlib.sha256(payload).hexdigest())
__CLIO_RELAY_BOOTSTRAP_PATH_SET_IDENTITY__
}}
BOOTSTRAP_TRANSACTION_JOURNAL="$HOME/.local/share/clio-relay/bootstrap-transaction.json"
BOOTSTRAP_RECOVERY_REQUIRED=0
if [ -L "$BOOTSTRAP_TRANSACTION_JOURNAL" ]; then
  echo "bootstrap transaction journal must not be a symbolic link" >&2
  exit 1
elif [ -f "$BOOTSTRAP_TRANSACTION_JOURNAL" ]; then
  BOOTSTRAP_RECOVERY_REQUIRED="$(
    python3 - "$BOOTSTRAP_TRANSACTION_JOURNAL" \
      <<'__CLIO_RELAY_RECOVERY_REQUIRED__'
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
if path.stat().st_size > 1024 * 1024:
    raise SystemExit("bootstrap transaction journal exceeds its bound")
value = json.loads(path.read_text(encoding="utf-8"))
state = value.get("state") if isinstance(value, dict) else None
print("0" if state in {{"committed", "recovered"}} else "1")
__CLIO_RELAY_RECOVERY_REQUIRED__
  )"
fi
export BOOTSTRAP_TRANSACTION_JOURNAL BOOTSTRAP_RECOVERY_REQUIRED
if [ "$BOOTSTRAP_RECOVERY_REQUIRED" = "1" ]; then
  BOOTSTRAP_EARLY_MODE="$(
    python3 - "$BOOTSTRAP_TRANSACTION_JOURNAL" \
      <<'__CLIO_RELAY_EARLY_RECOVERY_MODE__'
import json
import sys
from pathlib import Path

value = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
print(value.get("mode", "legacy") if isinstance(value, dict) else "invalid")
__CLIO_RELAY_EARLY_RECOVERY_MODE__
  )"
  if [ "$BOOTSTRAP_EARLY_MODE" = "full" ]; then
    BOOTSTRAP_EARLY_RECOVERY_JSON="$(
      bootstrap_journal_action recovery-plan "$BOOTSTRAP_TRANSACTION_JOURNAL"
    )"
    export BOOTSTRAP_EARLY_RECOVERY_JSON
    read -r BOOTSTRAP_EARLY_DIRECTION BOOTSTRAP_EARLY_SERVICE \
      BOOTSTRAP_EARLY_SERVICE_ACTIVE < <(
        python3 - <<'__CLIO_RELAY_EARLY_RECOVERY_FIELDS__'
import json
import os

value = json.loads(os.environ["BOOTSTRAP_EARLY_RECOVERY_JSON"])
service = value.get("service_name") or "-"
active = value.get("service_was_active")
active_text = "true" if active is True else ("false" if active is False else "unknown")
print(value["recovery_mode"], service, active_text)
__CLIO_RELAY_EARLY_RECOVERY_FIELDS__
      )
    if [ "$BOOTSTRAP_EARLY_DIRECTION" = "discard" ]; then
      bootstrap_journal_action discard-full "$BOOTSTRAP_TRANSACTION_JOURNAL" "$HOME"
      if [ "$BOOTSTRAP_EARLY_SERVICE_ACTIVE" = "true" ] && \
         [ "$BOOTSTRAP_EARLY_SERVICE" != "-" ]; then
        command -v timeout >/dev/null 2>&1 || {{
          echo "timeout is required for bootstrap service recovery" >&2
          exit 1
        }}
        timeout 55 systemctl --user start "$BOOTSTRAP_EARLY_SERVICE"
      fi
      exec 9>&-
      unset CLIO_RELAY_BOOTSTRAP_LOCK_FD
      exec bash "$0"
    fi
  fi
fi
BOOTSTRAP_CURRENT_RELAY="$HOME/.local/bin/clio-relay"
BOOTSTRAP_CURRENT_PROVIDER=""
BOOTSTRAP_LEGACY_RELAY_PROVIDER=1
BOOTSTRAP_INSTALL_RECEIPT="$HOME/.local/share/clio-relay/install-receipt.json"
if [ -e "$BOOTSTRAP_INSTALL_RECEIPT" ] || [ -L "$BOOTSTRAP_INSTALL_RECEIPT" ]; then
  if BOOTSTRAP_RELAY_RECEIPT_CLASS="$(
    env -u PYTHONPATH -u PYTHONHOME -u LD_PRELOAD -u LD_LIBRARY_PATH \
      python3 -I - "$BOOTSTRAP_INSTALL_RECEIPT" <<'__CLIO_RELAY_RECEIPT_CLASSIFY__'
{_BOOTSTRAP_RECEIPT_CLASSIFIER_SOURCE}
__CLIO_RELAY_RECEIPT_CLASSIFY__
  )"; then
    if [ "$BOOTSTRAP_RELAY_RECEIPT_CLASS" = "current" ]; then
      BOOTSTRAP_LEGACY_RELAY_PROVIDER=0
    fi
  else
    echo "bootstrap receipt classification failed; candidate payload is required" >&2
  fi
fi
if [ -x "$BOOTSTRAP_CURRENT_RELAY" ]; then
  BOOTSTRAP_CURRENT_PROVIDER="$(bootstrap_active_generation_provider 2>/dev/null || true)"
  if [ -z "$BOOTSTRAP_CURRENT_PROVIDER" ] && \
     [ -f "$UV_TOOL_DIR/clio-relay/bin/python" ] && \
     [ -x "$UV_TOOL_DIR/clio-relay/bin/python" ]; then
    BOOTSTRAP_CURRENT_PROVIDER="$UV_TOOL_DIR/clio-relay/bin/python"
  fi
fi
if [ "$BOOTSTRAP_RECOVERY_REQUIRED" = "0" ] && \
   [ "$BOOTSTRAP_LEGACY_RELAY_PROVIDER" = "0" ] && \
   [ -x "$BOOTSTRAP_CURRENT_RELAY" ]; then
  if [ -x "$BOOTSTRAP_CURRENT_PROVIDER" ] && \
     "$BOOTSTRAP_CURRENT_PROVIDER" -c \
       'from clio_relay.bootstrap_reconcile import BootstrapDesiredState' \
       >/dev/null 2>&1; then
    BOOTSTRAP_SERVICE_WAS_ACTIVE="unknown"
    BOOTSTRAP_SERVICE_WAS_ENABLED="unknown"
    if [ -n {shlex.quote(worker_service or "")} ]; then
      if systemctl --user is-active --quiet {shlex.quote(worker_service or "")}; then
        BOOTSTRAP_SERVICE_WAS_ACTIVE="true"
      else
        BOOTSTRAP_SERVICE_WAS_ACTIVE="false"
      fi
      if systemctl --user is-enabled --quiet {shlex.quote(worker_service or "")}; then
        BOOTSTRAP_SERVICE_WAS_ENABLED="true"
      else
        BOOTSTRAP_SERVICE_WAS_ENABLED="false"
      fi
    fi
    BOOTSTRAP_QUEUE_EVIDENCE=""
    BOOTSTRAP_WORKER_EVIDENCE=""
    if command -v timeout >/dev/null 2>&1; then
      BOOTSTRAP_QUEUE_EVIDENCE="$(
        CLIO_RELAY_CORE_DIR={rendered_core_dir} \
          timeout 20 "$BOOTSTRAP_CURRENT_RELAY" queue readiness-info 2>/dev/null || true
      )"
      if [ "$BOOTSTRAP_SERVICE_WAS_ACTIVE" = "true" ]; then
        BOOTSTRAP_WORKER_EVIDENCE="$(
          CLIO_RELAY_CORE_DIR={rendered_core_dir} \
            timeout 20 "$BOOTSTRAP_CURRENT_RELAY" endpoint worker-info \
              --cluster {shlex.quote(cluster or "")} --freshness-seconds 120 \
              --readiness-only 2>/dev/null || true
        )"
      fi
    fi
    export BOOTSTRAP_SERVICE_WAS_ACTIVE BOOTSTRAP_SERVICE_WAS_ENABLED
    export BOOTSTRAP_QUEUE_EVIDENCE BOOTSTRAP_WORKER_EVIDENCE
    set +e
    BOOTSTRAP_NOOP_OUTPUT="$(
      "$BOOTSTRAP_CURRENT_PROVIDER" - {invocation_id!r} <<'__CLIO_RELAY_BOOTSTRAP_NOOP__'
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path

from clio_relay.bootstrap_reconcile import (
    BootstrapDesiredState,
    inspect_exact_bootstrap_noop,
    make_bootstrap_receipt,
    write_bootstrap_receipt,
)

desired_payload = json.loads(os.environ["BOOTSTRAP_DESIRED_STATE"])
desired_payload["agent_npm_package"] = os.environ["AGENT_NPM_PACKAGE"] or None
desired_payload["agent_npm_bin"] = os.environ["AGENT_NPM_BIN"] or None
desired = BootstrapDesiredState.model_validate(desired_payload)
service_value = os.environ["BOOTSTRAP_SERVICE_WAS_ACTIVE"]
service_was_active = (
    True if service_value == "true" else (False if service_value == "false" else None)
)
enabled_value = os.environ["BOOTSTRAP_SERVICE_WAS_ENABLED"]
service_was_enabled = (
    True if enabled_value == "true" else (False if enabled_value == "false" else None)
)

def optional_json(name: str):
    value = os.environ[name]
    return json.loads(value) if value else None

inspection = inspect_exact_bootstrap_noop(
    desired,
    service_was_active=service_was_active,
    service_was_enabled=service_was_enabled,
    queue_evidence=optional_json("BOOTSTRAP_QUEUE_EVIDENCE"),
    worker_evidence=optional_json("BOOTSTRAP_WORKER_EVIDENCE"),
)
if inspection.exact_match:
    completed_ns = time.monotonic_ns()
    started_ns = int(os.environ["BOOTSTRAP_INVOCATION_STARTED_NS"])
    receipt = make_bootstrap_receipt(
        invocation_id=sys.argv[1],
        desired=desired,
        outcome="verified_after_transfer",
        inspection=inspection,
        started_at=datetime.fromisoformat(os.environ["BOOTSTRAP_INVOCATION_STARTED_AT"]),
        transaction=None,
        previous_generation=inspection.active_generation,
        active_generation=inspection.active_generation,
        duration_seconds=(completed_ns - started_ns) / 1_000_000_000,
        downloads=[],
        service_restart_count=0,
        payload_transfer_count=int(os.environ["BOOTSTRAP_PAYLOAD_TRANSFER_COUNT"]),
        payload_transfer_bytes=int(os.environ["BOOTSTRAP_PAYLOAD_TRANSFER_BYTES"]),
    )
    destination = Path.home() / ".local/share/clio-relay/bootstrap-receipt.json"
    write_bootstrap_receipt(destination, receipt)
    print(f"bootstrap_receipt={{destination}}")
    print("bootstrap_receipt_json=" + json.dumps(receipt, sort_keys=True, separators=(",", ":")))
else:
    print("bootstrap_reconcile_reasons=" + json.dumps(inspection.reasons, sort_keys=True))
__CLIO_RELAY_BOOTSTRAP_NOOP__
    )"
    BOOTSTRAP_NOOP_STATUS=$?
    set -e
    if [ "$BOOTSTRAP_NOOP_STATUS" -ne 0 ]; then
      echo "$BOOTSTRAP_NOOP_OUTPUT" >&2
      exit "$BOOTSTRAP_NOOP_STATUS"
    fi
    echo "$BOOTSTRAP_NOOP_OUTPUT"
    if printf '%s\n' "$BOOTSTRAP_NOOP_OUTPUT" | \
       grep -q '^bootstrap_receipt_json='; then
      exit 0
    fi
  fi
fi
JARVIS_STATE_ROOT="$HOME/.ppi-jarvis"
JARVIS_CONFIG_FILE="$JARVIS_STATE_ROOT/jarvis_config.yaml"
JARVIS_REPOS_FILE="$JARVIS_STATE_ROOT/repos.yaml"
JARVIS_GRAPH_FILE="$JARVIS_STATE_ROOT/resource_graph.yaml"
export JARVIS_STATE_ROOT JARVIS_CONFIG_FILE JARVIS_REPOS_FILE JARVIS_GRAPH_FILE
JARVIS_EXISTING_FILE_COUNT="$(python3 - <<'__CLIO_RELAY_JARVIS_STATE_CLASSIFY__'
import os
import stat
from pathlib import Path

"""
