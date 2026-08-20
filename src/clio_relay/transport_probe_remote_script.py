"""Remote FRP transport-probe bootstrap script.

Split out of ``transport_probe.py`` (iowarp/clio-relay#231): the single
function that renders the ``bash -s`` script the FRP HTTP probe pipes over
SSH -- it starts the owned ``clio-relay api`` process and the remote
``frpc`` proxy under a shared owner token, then writes the token-scoped
process-identity metadata ``_cleanup_remote_probe``
(``transport_probe_remote_cleanup.py``) later verifies before tearing
anything down. Not itself a monkeypatch seam, so a plain re-export back into
``transport_probe.py`` is enough for its resident caller,
``_run_frp_http_probe_with_proxy_type``, to keep resolving it.
"""

from __future__ import annotations

from clio_relay.cluster_config import ClusterDefinition
from clio_relay.remote_values import render_remote_shell_path, render_remote_shell_value
from clio_relay.transport_probe_primitives import _cluster_agent_bin, _shell_single_quote


def _remote_probe_script(
    *,
    cluster: str,
    definition: ClusterDefinition,
    probe_id: str,
    api_token: str | None,
    api_port: int,
    frpc_config: str,
) -> str:
    token_export = ""
    require_token = ""
    if api_token is not None:
        token_export = f"export CLIO_RELAY_API_TOKEN={_shell_single_quote(api_token)}"
        require_token = " --require-token"
    jarvis_bin = definition.jarvis_bin or "$HOME/.local/bin/jarvis"
    frpc_bin = definition.frpc_bin or "$HOME/.local/bin/frpc"
    agent_bin = _cluster_agent_bin(definition)
    return f"""set -euo pipefail
umask 077
export PATH="$HOME/.local/bin:$PATH"
export CLIO_RELAY_CORE_DIR={render_remote_shell_path(definition.core_dir, field="core_dir")}
export CLIO_RELAY_SPOOL_DIR={render_remote_shell_path(definition.spool_dir, field="spool_dir")}
export CLIO_RELAY_JARVIS_BIN={render_remote_shell_value(jarvis_bin, field="jarvis_bin")}
export CLIO_RELAY_FRPC_BIN={render_remote_shell_value(frpc_bin, field="frpc_bin")}
export CLIO_RELAY_AGENT_BIN={render_remote_shell_value(agent_bin, field="agent_bin")}
export CLIO_RELAY_AGENT_ADAPTER={_shell_single_quote(definition.agent_adapter)}
{token_export}
tmp="$(mktemp -d)"
probe_id={_shell_single_quote(probe_id)}
probe_dir="$HOME/.local/share/clio-relay/transport-probes/$probe_id"
metadata_file="$probe_dir/metadata.json"
mkdir -p "$probe_dir"
api_pid=""
frpc_pid=""
owner_token="$(python3 -c 'import secrets; print(secrets.token_hex(32))')"
cleanup() {{
  if [ -n "$frpc_pid" ]; then kill -- "-$frpc_pid" 2>/dev/null || true; fi
  if [ -n "$api_pid" ]; then kill -- "-$api_pid" 2>/dev/null || true; fi
  wait 2>/dev/null || true
  rm -rf "$tmp"
}}
trap cleanup EXIT
cat > "$tmp/frpc.toml" <<'__CLIO_RELAY_FRPC_CONFIG__'
{frpc_config.rstrip()}
__CLIO_RELAY_FRPC_CONFIG__
echo "transport_probe_cluster={cluster}"
if python3 - {api_port} <<'__CLIO_RELAY_PORT_CHECK__'
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
  echo "remote API port is already occupied: {api_port}" >&2
  exit 1
fi
setsid env "CLIO_RELAY_PROBE_OWNER_TOKEN=$owner_token" \
  clio-relay api start --host 127.0.0.1 --port {api_port}{require_token} \
  >"$probe_dir/api.log" 2>&1 &
api_pid="$!"
sleep 1
if ! kill -0 "$api_pid" 2>/dev/null; then
  cat "$probe_dir/api.log" >&2
  exit 1
fi
setsid env "CLIO_RELAY_PROBE_OWNER_TOKEN=$owner_token" \
  "$CLIO_RELAY_FRPC_BIN" -c "$tmp/frpc.toml" >"$probe_dir/frpc.log" 2>&1 &
frpc_pid="$!"
python3 - "$metadata_file" "$probe_id" "$owner_token" "$api_pid" "$frpc_pid" \
  "$tmp" "{api_port}" <<'__CLIO_RELAY_PROBE_METADATA__'
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

path, probe_id, owner_token, api_pid, frpc_pid, tmp, api_port = sys.argv[1:]

def identity(pid_raw, kind, markers):
    pid = int(pid_raw)
    proc = Path("/proc") / str(pid)
    for _ in range(40):
        try:
            pgid = os.getpgid(pid)
            environment = (proc / "environ").read_bytes().split(bytes([0]))
            command = (proc / "cmdline").read_bytes().replace(bytes([0]), b" ").decode(
                "utf-8", errors="replace"
            )
            start_ticks = (proc / "stat").read_text(encoding="utf-8").rsplit(")", 1)[1].split()[19]
        except OSError:
            time.sleep(0.05)
            continue
        owned = (
            pgid == pid
            and f"CLIO_RELAY_PROBE_OWNER_TOKEN={{owner_token}}".encode() in environment
            and all(marker in command for marker in markers)
        )
        if owned:
            return {{
                "kind": kind,
                "pid": pid,
                "pgid": pgid,
                "process_start_ticks": start_ticks,
                "command_contains": markers,
            }}
        time.sleep(0.05)
    raise RuntimeError(f"owned {{kind}} process did not establish its identity")

processes = [
    identity(api_pid, "remote_relay_api", ["clio-relay", "api", "start", "--port", api_port]),
    identity(frpc_pid, "remote_connector", ["frpc", f"{{tmp}}/frpc.toml"]),
]
metadata = {{
    "owner": "clio-relay",
    "probe_id": probe_id,
    "cluster": {cluster!r},
    "owner_token": owner_token,
    "tmp": tmp,
    "processes": processes,
    "logs": [
        str(Path(path).parent / "api.log"),
        str(Path(path).parent / "frpc.log"),
    ],
    "started_at": datetime.now(timezone.utc).isoformat(),
}}
temporary = Path(path).with_suffix(".tmp")
temporary.write_text(json.dumps(metadata, indent=2) + "\\n", encoding="utf-8")
os.replace(temporary, path)
__CLIO_RELAY_PROBE_METADATA__
wait
"""
