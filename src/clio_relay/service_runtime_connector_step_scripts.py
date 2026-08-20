"""Embedded remote shell/Python scripts for allocation-connector-step lifecycle.

Extracted from ``service_runtime.py`` (#231 rework slice): the SSH-delivered
script generators for the scheduler-allocation connector path -- querying,
canceling, and reconciling one scheduler connector step
(``_remote_connector_step_status_script``,
``_remote_connector_step_cancel_script``,
``_remote_connector_step_reconcile_script``), probing a remote service's HTTP
health endpoint (``_remote_http_probe_script``), and discovering/status-
checking one remote frpc connector by its durable, race-safe identity sidecar
(``_remote_connector_discovery_script``, ``_remote_connector_status_script``).
Every returned string is Python source embedded as heredoc text the remote
host runs, not live code in this module.

Depends only on ``clio_relay.cluster_config``/``clio_relay.remote_cli`` --
never on any other piece of the service-runtime split.
"""

from __future__ import annotations

import base64
import shlex

from clio_relay.cluster_config import ClusterDefinition
from clio_relay.remote_cli import remote_env


def _remote_connector_step_status_script(
    *,
    definition: ClusterDefinition,
    provider: str,
    scheduler_job_id: str,
    scheduler_step_id: str,
    placement_host: str,
) -> str:
    command = [
        "clio-relay",
        "scheduler",
        "connector-step-status",
        scheduler_step_id,
        "--scheduler-job-id",
        scheduler_job_id,
        "--cluster",
        definition.name,
        "--provider",
        provider,
        "--placement-host",
        placement_host,
    ]
    return f"set -euo pipefail\n{remote_env(definition)} {shlex.join(command)}\n"


def _remote_connector_step_cancel_script(
    *,
    definition: ClusterDefinition,
    provider: str,
    scheduler_job_id: str,
    scheduler_step_id: str,
) -> str:
    command = [
        "clio-relay",
        "scheduler",
        "connector-step-cancel",
        scheduler_step_id,
        "--scheduler-job-id",
        scheduler_job_id,
        "--cluster",
        definition.name,
        "--provider",
        provider,
    ]
    return f"set -euo pipefail\n{remote_env(definition)} {shlex.join(command)}\n"


def _remote_connector_step_reconcile_script(
    *,
    definition: ClusterDefinition,
    provider: str,
    scheduler_job_id: str,
    step_marker: str,
    placement_host: str,
) -> str:
    command = [
        "clio-relay",
        "scheduler",
        "connector-step-reconcile",
        scheduler_job_id,
        "--cluster",
        definition.name,
        "--provider",
        provider,
        "--placement-host",
        placement_host,
        "--step-marker",
        step_marker,
    ]
    return f"set -euo pipefail\n{remote_env(definition)} {shlex.join(command)}\n"


def _remote_http_probe_script(
    host: str,
    port: int,
    path: str,
    *,
    expected_body: str | None = None,
) -> str:
    encoded_body = (
        ""
        if expected_body is None
        else base64.b64encode(expected_body.encode("utf-8")).decode("ascii")
    )
    probe_arguments = shlex.join((host, str(port), path, encoded_body))
    return f"""set -euo pipefail
python3 - {probe_arguments} <<'__CLIO_SERVICE_HEALTH__'
import base64
import http.client
import sys
host, port, path, encoded_body = sys.argv[1], int(sys.argv[2]), sys.argv[3], sys.argv[4]
expected_body = base64.b64decode(encoded_body) if encoded_body else None
try:
    conn = http.client.HTTPConnection(host, port, timeout=5)
    conn.request("GET", path)
    response = conn.getresponse()
    body = response.read(4097)
    healthy = (
        200 <= response.status < 300
        and len(body) <= 4096
        and (expected_body is None or body == expected_body)
    )
    print(f"service_health={{'ok' if healthy else 'bad'}}")
    print(f"service_status={{response.status}}")
    conn.close()
except (OSError, http.client.HTTPException) as exc:
    print(f"service_health=unreachable")
    print(f"service_error={{exc}}")
__CLIO_SERVICE_HEALTH__
"""


def _remote_connector_discovery_script(
    *,
    session_id: str,
    owner_token: str,
    connector_generation_id: str,
    allocation_provider: str | None = None,
    allocation_job_id: str | None = None,
    allocation_step_marker: str | None = None,
    allocation_placement_host: str | None = None,
) -> str:
    """Discover one remote connector by its pre-recorded unforgeable identity."""
    return f"""set -euo pipefail
session_id={shlex.quote(session_id)}
owner_token={shlex.quote(owner_token)}
generation_id={shlex.quote(connector_generation_id)}
session_dir="$HOME/.local/share/clio-relay/service-sessions/$session_id"
python3 - "$session_dir" "$session_id" "$owner_token" "$generation_id" \
  {shlex.quote(allocation_provider or "")} {shlex.quote(allocation_job_id or "")} \
  {shlex.quote(allocation_step_marker or "")} \
  {shlex.quote(allocation_placement_host or "")} \
  <<'__CLIO_DISCOVER_CONNECTOR__'
import json
import os
import stat
import sys
from pathlib import Path

(
    session_dir,
    session_id,
    owner_token,
    generation_id,
    expected_provider,
    expected_job_id,
    expected_step_marker,
    expected_placement_host,
) = sys.argv[1:]
directory = Path(session_dir)
metadata_path = directory / "metadata.json"
try:
    metadata_before = os.lstat(metadata_path)
    if (
        not stat.S_ISREG(metadata_before.st_mode)
        or metadata_before.st_nlink != 1
        or metadata_before.st_mode & 0o077
        or metadata_before.st_size > 65536
    ):
        raise RuntimeError("remote connector sidecar is not a private bounded file")
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    metadata_after = os.lstat(metadata_path)
    if (
        metadata_before.st_ino,
        metadata_before.st_size,
        metadata_before.st_mtime_ns,
    ) != (
        metadata_after.st_ino,
        metadata_after.st_size,
        metadata_after.st_mtime_ns,
    ):
        raise RuntimeError("remote connector sidecar changed while reading")
except (FileNotFoundError, json.JSONDecodeError, OSError):
    metadata = None
if isinstance(metadata, dict) and metadata.get("schema_version") == (
    "clio-relay.allocation-connector-sidecar.v1"
):
    placement = metadata.get("placement")
    expected_identity = (
        bool(expected_provider)
        and bool(expected_job_id)
        and bool(expected_step_marker)
        and bool(expected_placement_host)
        and metadata.get("owner") == "clio-relay"
        and metadata.get("session_id") == session_id
        and metadata.get("owner_token") == owner_token
        and metadata.get("connector_generation_id") == generation_id
        and metadata.get("execution_scope") == "scheduler_allocation"
        and metadata.get("scheduler_provider") == expected_provider
        and metadata.get("scheduler_native_id") == expected_job_id
        and metadata.get("scheduler_step_marker") == expected_step_marker
        and isinstance(placement, dict)
        and placement.get("scheduler") == expected_provider
        and placement.get("scheduler_job_id") == expected_job_id
        and placement.get("placement_host") == expected_placement_host
        and placement.get("allocation_node_count") == 1
        and placement.get("verified") is True
        and isinstance(metadata.get("remote_frpc_config"), str)
        and isinstance(metadata.get("remote_frpc_log"), str)
    )
    if not expected_identity:
        print(json.dumps({{
            "present": False,
            "ownership_verified": False,
            "error": "allocation connector sidecar identity does not match its intent",
        }}))
        raise SystemExit(0)
    step = metadata.get("scheduler_step")
    if not isinstance(step, dict):
        for candidate_path in (
            directory / "scheduler-connector-step.json",
            directory / "scheduler-connector-step.pending.json",
            directory / "scheduler-connector-step.reconcile.json",
        ):
            try:
                candidate = json.loads(candidate_path.read_text(encoding="utf-8"))
            except (FileNotFoundError, json.JSONDecodeError, OSError):
                continue
            if isinstance(candidate, dict) and isinstance(candidate.get("step"), dict):
                candidate = candidate["step"]
            if isinstance(candidate, dict):
                step = candidate
                break
    connector = {{
        "owner": "clio-relay",
        "session_id": session_id,
        "execution_scope": "scheduler_allocation",
        "scheduler_provider": expected_provider,
        "scheduler_native_id": expected_job_id,
        "scheduler_step_marker": expected_step_marker,
        "connector_generation_id": generation_id,
        "owner_token": owner_token,
        "config_path": metadata["remote_frpc_config"],
        "log_path": metadata["remote_frpc_log"],
        "placement": placement,
    }}
    if isinstance(step, dict):
        connector["scheduler_step"] = step
        connector["scheduler_step_id"] = step.get("scheduler_step_id")
        print(json.dumps({{
            "present": True,
            "ownership_verified": True,
            "connector": connector,
        }}))
    else:
        print(json.dumps({{
            "present": False,
            "ownership_verified": True,
            "reconciliation_required": True,
            "connector": connector,
        }}))
    raise SystemExit(0)
token_marker = f"CLIO_RELAY_CONNECTOR_OWNER_TOKEN={{owner_token}}".encode()
generation_marker = f"CLIO_RELAY_CONNECTOR_GENERATION_ID={{generation_id}}".encode()
matches = []
observation_errors = []
for proc in Path("/proc").iterdir():
    if not proc.name.isdigit():
        continue
    try:
        if proc.stat().st_uid != os.geteuid():
            continue
    except FileNotFoundError:
        continue
    except OSError as exc:
        observation_errors.append(f"{{proc.name}}: owner lookup failed: {{exc}}")
        continue
    try:
        environment = (proc / "environ").read_bytes().split(bytes([0]))
        state = (proc / "stat").read_text(encoding="utf-8").rsplit(")", 1)[1].split()[0]
    except FileNotFoundError:
        continue
    except (OSError, IndexError) as exc:
        observation_errors.append(f"{{proc.name}}: identity read failed: {{exc}}")
        continue
    if state != "Z" and token_marker in environment and generation_marker in environment:
        matches.append(int(proc.name))
matches.sort()
if observation_errors:
    print(json.dumps({{
        "present": bool(matches),
        "ownership_verified": False,
        "matching_pids": matches,
        "error": "remote connector observation was incomplete: " + "; ".join(
            observation_errors[:20]
        ),
    }}))
    raise SystemExit(0)
if len(matches) > 1:
    print(json.dumps({{
        "present": True,
        "ownership_verified": False,
        "matching_pids": matches,
        "error": "multiple processes matched one connector intent",
    }}))
    raise SystemExit(0)
if len(matches) == 0:
    print(json.dumps({{
        "present": False,
        "ownership_verified": True,
        "matching_pids": [],
    }}))
    raise SystemExit(0)
pid = matches[0]
pgid = os.getpgid(pid)
if not isinstance(metadata, dict):
    metadata = {{
        "owner": "clio-relay",
        "session_id": session_id,
        "remote_frpc_pid": pid,
        "remote_frpc_pgid": pgid,
        "remote_frpc_config": str(directory / "remote-frpc.toml"),
        "remote_frpc_log": str(directory / "remote-frpc.log"),
        "owner_token": owner_token,
        "connector_generation_id": generation_id,
    }}
identity_valid = (
    metadata.get("owner") == "clio-relay"
    and metadata.get("session_id") == session_id
    and metadata.get("owner_token") == owner_token
    and metadata.get("connector_generation_id") == generation_id
    and metadata.get("remote_frpc_pid") == pid
    and metadata.get("remote_frpc_pgid") == pgid
)
connector = {{
    "owner": "clio-relay",
    "session_id": session_id,
    "pid": pid,
    "process_group_id": pgid,
    "connector_generation_id": generation_id,
    "owner_token": owner_token,
    "config_path": metadata.get("remote_frpc_config"),
    "log_path": metadata.get("remote_frpc_log"),
}}
print(json.dumps({{
    "present": True,
    "ownership_verified": identity_valid,
    "matching_pids": matches,
    "connector": connector,
}}))
__CLIO_DISCOVER_CONNECTOR__
"""


def _remote_connector_status_script(*, session_id: str, pid: int) -> str:
    return f"""set -euo pipefail
session_id={shlex.quote(session_id)}
pid={pid}
metadata_file="$HOME/.local/share/clio-relay/service-sessions/$session_id/metadata.json"
python3 - "$metadata_file" "$pid" "$session_id" <<'__CLIO_CONNECTOR_STATUS__'
import json
import os
import sys
from pathlib import Path

metadata_path, pid_raw, session_id = sys.argv[1:]
pid = int(pid_raw)
try:
    metadata = json.loads(Path(metadata_path).read_text(encoding="utf-8"))
except (FileNotFoundError, json.JSONDecodeError) as exc:
    raise RuntimeError("durable connector ownership metadata is unavailable") from exc
token = metadata.get("owner_token")
generation_id = metadata.get("connector_generation_id")
pgid = metadata.get("remote_frpc_pgid")
config_path = metadata.get("remote_frpc_config")
durable = (
    metadata.get("owner") == "clio-relay"
    and metadata.get("session_id") == session_id
    and metadata.get("remote_frpc_pid") == pid
    and isinstance(token, str) and bool(token)
    and isinstance(generation_id, str) and bool(generation_id)
    and isinstance(pgid, int)
    and isinstance(config_path, str)
)
matches = []
if durable:
    token_marker = f"CLIO_RELAY_CONNECTOR_OWNER_TOKEN={{token}}".encode()
    generation_marker = f"CLIO_RELAY_CONNECTOR_GENERATION_ID={{generation_id}}".encode()
    for proc in Path("/proc").iterdir():
        if not proc.name.isdigit():
            continue
        member_pid = int(proc.name)
        try:
            if proc.stat().st_uid != os.geteuid():
                continue
            fields = (proc / "stat").read_text(encoding="utf-8").rsplit(")", 1)[1].split()
            process_group = os.getpgid(member_pid)
        except (FileNotFoundError, ProcessLookupError):
            continue
        except (OSError, IndexError, ValueError) as exc:
            raise RuntimeError(
                f"cannot inspect remote connector status candidate {{member_pid}}: {{exc}}"
            ) from exc
        if fields[0] == "Z" or process_group != pgid:
            continue
        try:
            environment = (proc / "environ").read_bytes().split(bytes([0]))
            command = (proc / "cmdline").read_bytes().replace(bytes([0]), b" ").decode(
                "utf-8", errors="replace"
            )
        except (FileNotFoundError, ProcessLookupError):
            continue
        except OSError as exc:
            raise RuntimeError(
                f"cannot verify remote connector status candidate {{member_pid}}: {{exc}}"
            ) from exc
        if (
            token_marker in environment
            and generation_marker in environment
            and "frpc" in command
            and config_path in command
        ):
            matches.append(member_pid)
print(json.dumps({{
    "pid": pid,
    "ownership_verified": durable and bool(matches),
    "running": bool(matches),
    "matching_pids": sorted(matches),
}}))
__CLIO_CONNECTOR_STATUS__
"""
