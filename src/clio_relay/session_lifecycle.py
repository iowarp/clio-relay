"""Owned remote relay session lifecycle helpers."""

from __future__ import annotations

import json
import shlex
import subprocess
from dataclasses import dataclass
from typing import cast

from clio_relay.cluster_config import ClusterDefinition
from clio_relay.errors import RelayError
from clio_relay.remote_cli import remote_env


@dataclass(frozen=True)
class RemoteSession:
    """A remotely owned relay session."""

    session_id: str
    remote_api_port: int
    api_token: str | None


def start_remote_session(
    *,
    cluster: str,
    definition: ClusterDefinition,
    session_id: str,
    remote_api_port: int,
    api_token: str | None,
    replace: bool = False,
) -> list[str]:
    """Start a cluster-side relay API owned by a session id."""
    _validate_session(session_id=session_id, remote_api_port=remote_api_port)
    result = _ssh_script(
        definition,
        _start_script(
            cluster=cluster,
            definition=definition,
            session_id=session_id,
            remote_api_port=remote_api_port,
            api_token=api_token,
            replace=replace,
        ),
    )
    return result.splitlines()


def status_remote_session(
    *,
    definition: ClusterDefinition,
    session_id: str,
) -> dict[str, object]:
    """Return status for a previously started remote relay session."""
    _validate_session(session_id=session_id, remote_api_port=1)
    output = _ssh_script(definition, _status_script(session_id=session_id))
    return cast(dict[str, object], json.loads(output))


def teardown_remote_session(
    *,
    definition: ClusterDefinition,
    session_id: str,
    stop_worker: bool = False,
    cluster: str | None = None,
) -> list[str]:
    """Stop processes owned by a remote relay session."""
    _validate_session(session_id=session_id, remote_api_port=1)
    output = _ssh_script(
        definition,
        _teardown_script(session_id=session_id, stop_worker=stop_worker, cluster=cluster),
    )
    return output.splitlines()


def _start_script(
    *,
    cluster: str,
    definition: ClusterDefinition,
    session_id: str,
    remote_api_port: int,
    api_token: str | None,
    replace: bool,
) -> str:
    token_export = ""
    require_token = ""
    if api_token is not None:
        token_export = f"export CLIO_RELAY_API_TOKEN={_shell_single_quote(api_token)}"
        require_token = " --require-token"
    replace_flag = "1" if replace else "0"
    return f"""set -euo pipefail
{remote_env(definition)}
{token_export}
session_id={shlex.quote(session_id)}
session_dir="$HOME/.local/share/clio-relay/sessions/$session_id"
mkdir -p "$session_dir"
pid_file="$session_dir/api.pid"
log_file="$session_dir/api.log"
metadata_file="$session_dir/metadata.json"
if [ -s "$pid_file" ] && kill -0 "$(cat "$pid_file")" 2>/dev/null; then
  if [ "{replace_flag}" != "1" ]; then
    echo "session_already_running=$session_id"
    echo "api_pid=$(cat "$pid_file")"
    exit 0
  fi
  kill "$(cat "$pid_file")" 2>/dev/null || true
fi
if python3 - {remote_api_port} <<'__CLIO_RELAY_PORT_CHECK__'
import socket
import sys
port = int(sys.argv[1])
with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        sock.bind(("127.0.0.1", port))
    except OSError:
        raise SystemExit(1)
__CLIO_RELAY_PORT_CHECK__
then
  :
else
  echo "remote API port is already occupied: {remote_api_port}" >&2
  exit 1
fi
api_command=(clio-relay api start --host 127.0.0.1 --port {remote_api_port}{require_token})
nohup "${{api_command[@]}}" >"$log_file" 2>&1 &
api_pid="$!"
echo "$api_pid" > "$pid_file"
python3 - "$metadata_file" "$api_pid" <<'__CLIO_RELAY_METADATA__'
import json
import sys
from datetime import datetime, timezone
path = sys.argv[1]
api_pid = int(sys.argv[2])
metadata = {{
    "cluster": {cluster!r},
    "session_id": {session_id!r},
    "remote_api_port": {remote_api_port},
    "api_pid": api_pid,
    "started_at": datetime.now(timezone.utc).isoformat(),
    "owner": "clio-relay",
}}
with open(path, "w", encoding="utf-8") as handle:
    json.dump(metadata, handle, indent=2)
__CLIO_RELAY_METADATA__
sleep 1
if ! kill -0 "$api_pid" 2>/dev/null; then
  cat "$log_file" >&2
  exit 1
fi
echo "session_started=$session_id"
echo "api_pid=$api_pid"
echo "remote_api_port={remote_api_port}"
echo "metadata=$metadata_file"
"""


def _status_script(*, session_id: str) -> str:
    return f"""set -euo pipefail
session_id={shlex.quote(session_id)}
session_dir="$HOME/.local/share/clio-relay/sessions/$session_id"
pid_file="$session_dir/api.pid"
metadata_file="$session_dir/metadata.json"
running=false
api_pid=null
if [ -s "$pid_file" ]; then
  api_pid="$(cat "$pid_file")"
  if kill -0 "$api_pid" 2>/dev/null; then running=true; fi
fi
python3 - "$metadata_file" "$running" "$api_pid" <<'__CLIO_RELAY_STATUS__'
import json
import sys
metadata_path, running, api_pid = sys.argv[1:]
metadata = {{}}
try:
    with open(metadata_path, encoding="utf-8") as handle:
        metadata = json.load(handle)
except FileNotFoundError:
    pass
metadata["running"] = running == "true"
metadata["api_pid"] = None if api_pid == "null" else int(api_pid)
print(json.dumps(metadata))
__CLIO_RELAY_STATUS__
"""


def _teardown_script(*, session_id: str, stop_worker: bool, cluster: str | None) -> str:
    worker_command = ""
    if stop_worker:
        if cluster is None:
            raise RelayError("cluster is required when stopping the worker service")
        service = shlex.quote(f"clio-relay-worker-{cluster}.service")
        worker_command = f"systemctl --user stop {service} || true\necho worker_stopped={service}\n"
    return f"""set -euo pipefail
session_id={shlex.quote(session_id)}
session_dir="$HOME/.local/share/clio-relay/sessions/$session_id"
pid_file="$session_dir/api.pid"
if [ -s "$pid_file" ]; then
  api_pid="$(cat "$pid_file")"
  if kill -0 "$api_pid" 2>/dev/null; then
    kill "$api_pid" 2>/dev/null || true
    for _ in 1 2 3 4 5; do
      if ! kill -0 "$api_pid" 2>/dev/null; then break; fi
      sleep 1
    done
    if kill -0 "$api_pid" 2>/dev/null; then kill -9 "$api_pid" 2>/dev/null || true; fi
    echo "api_stopped=$api_pid"
  else
    echo "api_not_running=$api_pid"
  fi
else
  echo "api_pid_missing=$session_id"
fi
rm -f "$pid_file"
{worker_command}echo "session_teardown=$session_id"
"""


def _validate_session(*, session_id: str, remote_api_port: int) -> None:
    if not session_id or not all(item.isalnum() or item in {"-", "_"} for item in session_id):
        raise RelayError("session_id must contain only letters, numbers, hyphen, or underscore")
    if remote_api_port <= 0:
        raise RelayError("remote_api_port must be positive")


def _ssh_script(definition: ClusterDefinition, script: str) -> str:
    result = subprocess.run(
        ["ssh", definition.ssh_host, "bash", "-s"],
        input=script.encode("utf-8"),
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        stdout = result.stdout.decode("utf-8", errors="replace").strip()
        stderr = result.stderr.decode("utf-8", errors="replace").strip()
        detail = stderr or stdout
        raise RelayError(f"remote session command failed: {detail}")
    return result.stdout.decode("utf-8", errors="replace")


def _shell_single_quote(value: str) -> str:
    return "'" + value.replace("'", "'\"'\"'") + "'"
