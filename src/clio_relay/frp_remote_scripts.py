"""Remote shell programs for durable service-runtime frp connectors."""

from __future__ import annotations

import base64
import shlex

from clio_relay.cluster_config import ClusterDefinition
from clio_relay.errors import ConfigurationError
from clio_relay.models import SchedulerConnectorPlacement
from clio_relay.remote_cli import remote_env
from clio_relay.remote_values import render_remote_shell_value

_LINUX_PIDFD_SEND_SIGNAL_SYSCALL_NUMBER = 424
_LINUX_PIDFD_OPEN_SYSCALL_NUMBER = 434


def remote_allocation_frpc_start_script(
    *,
    definition: ClusterDefinition,
    session_id: str,
    config_text: str,
    owner_token: str,
    connector_generation_id: str,
    allocation_provider: str,
    allocation_job_id: str,
    placement: SchedulerConnectorPlacement,
    step_marker: str,
) -> str:
    """Launch frpc as a durable scheduler step, never as a login-node child PID."""
    encoded = base64.b64encode(config_text.encode("utf-8")).decode("ascii")
    frpc_bin = definition.frpc_bin or "$HOME/.local/bin/frpc"
    if (
        placement.scheduler != allocation_provider
        or placement.scheduler_job_id != allocation_job_id
    ):
        raise ConfigurationError("connector placement does not match its allocation")
    placement_host = placement.placement_host
    placement_json = placement.model_dump_json()
    return f"""set -euo pipefail
umask 077
{remote_env(definition)}
session_id={shlex.quote(session_id)}
session_dir="$HOME/.local/share/clio-relay/service-sessions/$session_id"
mkdir -p "$session_dir"
chmod 700 "$session_dir"
exec 9>"$session_dir/transition.lock"
flock -w 10 -x 9 || {{ echo "connector start lock timed out" >&2; exit 75; }}
config_file="$session_dir/remote-frpc.toml"
log_file="$session_dir/remote-frpc.log"
metadata_file="$session_dir/metadata.json"
step_file="$session_dir/scheduler-connector-step.json"
pending_file="$session_dir/scheduler-connector-step.pending.json"
reconcile_file="$session_dir/scheduler-connector-step.reconcile.json"
reconcile_state_file="$session_dir/scheduler-connector-step.reconcile-state"
python3 - "$config_file" <<'__CLIO_WRITE_ALLOCATION_FRPC__'
import base64
import os
import sys
from pathlib import Path

path = Path(sys.argv[1])
temporary = path.with_name(f".{{path.name}}.{{os.getpid()}}.tmp")
with temporary.open("w", encoding="utf-8") as handle:
    handle.write(base64.b64decode({encoded!r}).decode("utf-8"))
    handle.flush()
    os.fsync(handle.fileno())
os.chmod(temporary, 0o600)
os.replace(temporary, path)
directory = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
try:
    os.fsync(directory)
finally:
    os.close(directory)
__CLIO_WRITE_ALLOCATION_FRPC__
python3 - "$metadata_file" "$session_id" "$config_file" "$log_file" \
  {shlex.quote(owner_token)} {shlex.quote(connector_generation_id)} \
  {shlex.quote(allocation_provider)} {shlex.quote(allocation_job_id)} \
  {shlex.quote(placement_host)} {shlex.quote(step_marker)} \
  {shlex.quote(placement_json)} <<'__CLIO_ALLOCATION_INTENT__'
import json
import os
import stat
import sys
from datetime import datetime, timezone
from pathlib import Path

(
    metadata_raw,
    session_id,
    config_path,
    log_path,
    owner_token,
    generation_id,
    provider,
    job_id,
    placement_host,
    step_marker,
    placement_raw,
) = sys.argv[1:]
path = Path(metadata_raw)
expected = {{
    "schema_version": "clio-relay.allocation-connector-sidecar.v1",
    "owner": "clio-relay",
    "session_id": session_id,
    "owner_token": owner_token,
    "connector_generation_id": generation_id,
    "execution_scope": "scheduler_allocation",
    "scheduler_provider": provider,
    "scheduler_native_id": job_id,
    "scheduler_step_marker": step_marker,
    "placement": json.loads(placement_raw),
    "remote_frpc_config": config_path,
    "remote_frpc_log": log_path,
}}
try:
    before = os.lstat(path)
except FileNotFoundError:
    current = None
else:
    if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1 or before.st_mode & 0o077:
        raise RuntimeError("allocation connector sidecar is not a private regular file")
    if before.st_size > 65536:
        raise RuntimeError("allocation connector sidecar exceeds its size bound")
    current = json.loads(path.read_text(encoding="utf-8"))
    after = os.lstat(path)
    if (before.st_ino, before.st_size, before.st_mtime_ns) != (
        after.st_ino,
        after.st_size,
        after.st_mtime_ns,
    ):
        raise RuntimeError("allocation connector sidecar changed while reading")
    if not isinstance(current, dict) or any(
        current.get(key) != value for key, value in expected.items()
    ):
        raise RuntimeError("allocation connector sidecar identity does not match launch intent")
payload = {{
    **expected,
    "state": "starting",
    "scheduler_step": current.get("scheduler_step") if isinstance(current, dict) else None,
    "updated_at": datetime.now(timezone.utc).isoformat(),
}}
temporary = path.with_name(f".{{path.name}}.{{os.getpid()}}.tmp")
with temporary.open("w", encoding="utf-8") as handle:
    json.dump(payload, handle, sort_keys=True)
    handle.flush()
    os.fsync(handle.fileno())
os.chmod(temporary, 0o600)
os.replace(temporary, path)
directory = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
try:
    os.fsync(directory)
finally:
    os.close(directory)
__CLIO_ALLOCATION_INTENT__
clio-relay scheduler connector-step-reconcile \
  {shlex.quote(allocation_job_id)} \
  --cluster {shlex.quote(definition.name)} \
  --provider {shlex.quote(allocation_provider)} \
  --placement-host {shlex.quote(placement_host)} \
  --step-marker {shlex.quote(step_marker)} >"$reconcile_file"
python3 - "$reconcile_file" \
  {shlex.quote(allocation_provider)} {shlex.quote(allocation_job_id)} \
  {shlex.quote(placement_host)} {shlex.quote(step_marker)} \
  >"$reconcile_state_file" \
  <<'__CLIO_RECONCILE_STATE__'
import json
import sys
from pathlib import Path

path, provider, job_id, placement_host, step_marker = sys.argv[1:]
record = json.loads(Path(path).read_text(encoding="utf-8"))
valid = (
    isinstance(record, dict)
    and record.get("schema_version")
    == "clio-relay.scheduler-connector-step-reconciliation.v1"
    and record.get("scheduler") == provider
    and record.get("scheduler_job_id") == job_id
    and record.get("placement_host") == placement_host
    and record.get("step_marker") == step_marker
    and isinstance(record.get("found"), bool)
)
if not valid:
    raise RuntimeError("connector step reconciliation identity is invalid")
print("found" if record["found"] else "absent")
__CLIO_RECONCILE_STATE__
reconcile_state=""
IFS= read -r reconcile_state <"$reconcile_state_file"
if [ "$reconcile_state" != "found" ] && [ "$reconcile_state" != "absent" ]; then
  echo "connector step reconciliation returned an invalid state" >&2
  exit 75
fi
candidate_file="$reconcile_file"
candidate_kind="reconciliation"
if [ "$reconcile_state" = "absent" ]; then
  frpc_bin={render_remote_shell_value(frpc_bin, field="frpc_bin")}
  clio-relay scheduler connector-step-start \
    {shlex.quote(allocation_job_id)} \
    --cluster {shlex.quote(definition.name)} \
    --provider {shlex.quote(allocation_provider)} \
    --placement-host {shlex.quote(placement_host)} \
    --step-marker {shlex.quote(step_marker)} \
    --output-path "$log_file" -- \
    "$frpc_bin" -c "$config_file" >"$pending_file"
  candidate_file="$pending_file"
  candidate_kind="launch"
fi
python3 - "$candidate_file" "$candidate_kind" "$metadata_file" "$step_file" \
  "$session_id" "$config_file" "$log_file" \
  {shlex.quote(owner_token)} {shlex.quote(connector_generation_id)} \
  {shlex.quote(allocation_provider)} {shlex.quote(allocation_job_id)} \
  {shlex.quote(placement_host)} {shlex.quote(step_marker)} \
  <<'__CLIO_RECORD_ALLOCATION_STEP__'
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

(
    candidate_raw,
    candidate_kind,
    metadata_raw,
    step_raw,
    session_id,
    config_path,
    log_path,
    owner_token,
    generation_id,
    provider,
    job_id,
    placement_host,
    step_marker,
) = sys.argv[1:]
candidate = json.loads(Path(candidate_raw).read_text(encoding="utf-8"))
step = candidate.get("step") if candidate_kind == "reconciliation" else candidate
if not isinstance(step, dict):
    raise RuntimeError("scheduler connector launch omitted its step identity")
expected_step_prefix = f"{{job_id}}."
step_id = step.get("scheduler_step_id")
valid_step = (
    step.get("schema_version") == "clio-relay.scheduler-connector-step.v1"
    and step.get("scheduler") == provider
    and step.get("scheduler_job_id") == job_id
    and isinstance(step_id, str)
    and step_id.startswith(expected_step_prefix)
    and step_id[len(expected_step_prefix):].isdecimal()
    and step.get("step_marker") == step_marker
    and step.get("placement_host") == placement_host
    and step.get("source")
    in {{"slurm-srun-detached-marker", "slurm-squeue-step-marker"}}
    and step.get("verified") is True
)
if not valid_step:
    raise RuntimeError("scheduler connector step identity does not match launch intent")
metadata_path = Path(metadata_raw)
metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
expected_metadata = {{
    "schema_version": "clio-relay.allocation-connector-sidecar.v1",
    "owner": "clio-relay",
    "session_id": session_id,
    "owner_token": owner_token,
    "connector_generation_id": generation_id,
    "execution_scope": "scheduler_allocation",
    "scheduler_provider": provider,
    "scheduler_native_id": job_id,
    "scheduler_step_marker": step_marker,
    "remote_frpc_config": config_path,
    "remote_frpc_log": log_path,
}}
if not isinstance(metadata, dict) or any(
    metadata.get(key) != value for key, value in expected_metadata.items()
):
    raise RuntimeError("allocation connector sidecar changed before step recording")

def atomic_json(path, payload):
    temporary = path.with_name(f".{{path.name}}.{{os.getpid()}}.tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, sort_keys=True)
        handle.flush()
        os.fsync(handle.fileno())
    os.chmod(temporary, 0o600)
    os.replace(temporary, path)

atomic_json(Path(step_raw), step)
metadata["state"] = "recorded"
metadata["scheduler_step"] = step
metadata["updated_at"] = datetime.now(timezone.utc).isoformat()
atomic_json(metadata_path, metadata)
directory = os.open(metadata_path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
try:
    os.fsync(directory)
finally:
    os.close(directory)
print(json.dumps({{
    "schema_version": "clio-relay.allocation-connector-start.v1",
    "session_id": session_id,
    "connector_generation_id": generation_id,
    "config_path": config_path,
    "log_path": log_path,
    "step_identity": step,
}}))
__CLIO_RECORD_ALLOCATION_STEP__
rm -f -- "$pending_file" "$reconcile_file" "$reconcile_state_file"
"""


def remote_frpc_start_script(
    *,
    definition: ClusterDefinition,
    session_id: str,
    config_text: str,
    owner_token: str,
    connector_generation_id: str,
) -> str:
    encoded = base64.b64encode(config_text.encode("utf-8")).decode("ascii")
    frpc_bin = definition.frpc_bin or "$HOME/.local/bin/frpc"
    return f"""set -euo pipefail
umask 077
{remote_env(definition)}
session_id={shlex.quote(session_id)}
session_dir="$HOME/.local/share/clio-relay/service-sessions/$session_id"
mkdir -p "$session_dir"
exec 9>"$session_dir/transition.lock"
flock -w 10 -x 9 || {{ echo "connector start lock timed out" >&2; exit 75; }}
config_file="$session_dir/remote-frpc.toml"
log_file="$session_dir/remote-frpc.log"
pid_file="$session_dir/remote-frpc.pid"
metadata_file="$session_dir/metadata.json"
python3 - "$metadata_file" "$pid_file" "$session_id" <<'__CLIO_CONNECTOR_PREFLIGHT__'
import json
import os
import sys
from pathlib import Path

metadata_path, pid_path, session_id = sys.argv[1:]
try:
    metadata = json.loads(Path(metadata_path).read_text(encoding="utf-8"))
except (FileNotFoundError, json.JSONDecodeError):
    metadata = {{}}
token = metadata.get("owner_token")
generation_id = metadata.get("connector_generation_id")
pgid = metadata.get("remote_frpc_pgid")
recorded_pid = metadata.get("remote_frpc_pid")
if not isinstance(recorded_pid, int):
    try:
        recorded_pid = int(Path(pid_path).read_text(encoding="utf-8").strip())
    except (OSError, ValueError):
        recorded_pid = None
active_recorded_pid = False
if isinstance(recorded_pid, int):
    try:
        state = (Path("/proc") / str(recorded_pid) / "stat").read_text(
            encoding="utf-8"
        ).rsplit(")", 1)[1].split()[0]
        active_recorded_pid = state != "Z"
    except (OSError, IndexError):
        pass
matches = []
active_group_pids = []
complete_identity = (
    metadata.get("owner") == "clio-relay"
    and metadata.get("session_id") == session_id
    and isinstance(token, str)
    and token
    and isinstance(generation_id, str)
    and generation_id
    and isinstance(pgid, int)
)
if isinstance(pgid, int):
    token_marker = (
        f"CLIO_RELAY_CONNECTOR_OWNER_TOKEN={{token}}".encode()
        if complete_identity
        else None
    )
    generation_marker = (
        f"CLIO_RELAY_CONNECTOR_GENERATION_ID={{generation_id}}".encode()
        if complete_identity
        else None
    )
    for proc in Path("/proc").iterdir():
        if not proc.name.isdigit():
            continue
        member_pid = int(proc.name)
        try:
            state = (proc / "stat").read_text(encoding="utf-8").rsplit(")", 1)[1].split()[0]
            process_group = os.getpgid(member_pid)
        except (FileNotFoundError, ProcessLookupError):
            continue
        except (OSError, IndexError, ValueError) as exc:
            raise RuntimeError(
                f"cannot inspect prior connector candidate {{member_pid}}: {{exc}}"
            ) from exc
        if state == "Z" or process_group != pgid:
            continue
        active_group_pids.append(member_pid)
        try:
            environment = (proc / "environ").read_bytes().split(bytes([0]))
        except (FileNotFoundError, ProcessLookupError):
            continue
        except OSError as exc:
            raise RuntimeError(
                f"cannot verify prior connector candidate {{member_pid}}: {{exc}}"
            ) from exc
        if (
            token_marker is not None
            and generation_marker is not None
            and token_marker in environment
            and generation_marker in environment
        ):
            matches.append(member_pid)
if matches:
    raise RuntimeError(f"owned remote connector is already active: pids={{matches}}")
if active_recorded_pid or active_group_pids:
    raise RuntimeError(
        "refusing to replace an active remote connector without complete ownership proof: "
        f"pid={{recorded_pid}} group_pids={{active_group_pids}}"
    )
Path(pid_path).unlink(missing_ok=True)
__CLIO_CONNECTOR_PREFLIGHT__
python3 - "$config_file" <<'__CLIO_WRITE_FRPC__'
import base64
import sys
path = sys.argv[1]
data = base64.b64decode({encoded!r}).decode("utf-8")
with open(path, "w", encoding="utf-8") as handle:
    handle.write(data)
__CLIO_WRITE_FRPC__
frpc_bin={render_remote_shell_value(frpc_bin, field="frpc_bin")}
owner_token={shlex.quote(owner_token)}
connector_generation_id={shlex.quote(connector_generation_id)}
pid=""
start_complete=0
cleanup_incomplete_start() {{
  if [ "$start_complete" = "1" ] || [ -z "$pid" ]; then return; fi
  kill -- "-$pid" 2>/dev/null || kill "$pid" 2>/dev/null || true
  for _ in 1 2 3 4 5; do
    if ! kill -0 -- "-$pid" 2>/dev/null; then break; fi
    sleep 0.2
  done
  if kill -0 -- "-$pid" 2>/dev/null; then
    kill -9 -- "-$pid" 2>/dev/null || kill -9 "$pid" 2>/dev/null || true
  fi
  for _ in 1 2 3 4 5; do
    if ! kill -0 -- "-$pid" 2>/dev/null; then break; fi
    sleep 0.1
  done
  if kill -0 -- "-$pid" 2>/dev/null; then
    echo "incomplete remote connector process group cleanup: $pid" >&2
    return 1
  fi
  python3 - \
    "$metadata_file" "$pid_file" "$pid" "$connector_generation_id" \
    <<'__CLIO_CONNECTOR_ROLLBACK__'
import json
import sys
from pathlib import Path

metadata_path, pid_path, pid_raw, generation_id = sys.argv[1:]
metadata_file = Path(metadata_path)
try:
    metadata = json.loads(metadata_file.read_text(encoding="utf-8"))
except (FileNotFoundError, json.JSONDecodeError):
    metadata = None
if (
    isinstance(metadata, dict)
    and str(metadata.get("remote_frpc_pid")) == pid_raw
    and metadata.get("connector_generation_id") == generation_id
):
    metadata_file.unlink(missing_ok=True)
try:
    recorded_pid = Path(pid_path).read_text(encoding="utf-8").strip()
except OSError:
    recorded_pid = None
if recorded_pid == pid_raw:
    Path(pid_path).unlink(missing_ok=True)
__CLIO_CONNECTOR_ROLLBACK__
}}
trap cleanup_incomplete_start EXIT
nohup setsid env \
  "CLIO_RELAY_CONNECTOR_OWNER_TOKEN=$owner_token" \
  "CLIO_RELAY_CONNECTOR_GENERATION_ID=$connector_generation_id" \
  "$frpc_bin" -c "$config_file" >"$log_file" 2>&1 9>&- &
pid="$!"
echo "$pid" > "$pid_file"
python3 - "$metadata_file" "$pid" "$config_file" "$log_file" \
  "$owner_token" "$connector_generation_id" <<'__CLIO_METADATA__'
import json
import os
import sys
import time
from datetime import datetime, timezone
metadata_file, pid, config_file, log_file, owner_token, generation_id = sys.argv[1:]
pid_value = int(pid)
for _ in range(40):
    try:
        process_group_id = os.getpgid(pid_value)
        with open(f"/proc/{{pid}}/environ", "rb") as handle:
            environment = handle.read().split(bytes([0]))
    except OSError:
        time.sleep(0.05)
        continue
    if (
        process_group_id == pid_value
        and f"CLIO_RELAY_CONNECTOR_OWNER_TOKEN={{owner_token}}".encode() in environment
        and f"CLIO_RELAY_CONNECTOR_GENERATION_ID={{generation_id}}".encode() in environment
    ):
        break
    time.sleep(0.05)
else:
    raise RuntimeError("owned connector did not establish its isolated process group")
with open(f"/proc/{{pid}}/stat", encoding="utf-8") as handle:
    process_start_ticks = handle.read().rsplit(")", 1)[1].split()[19]
temporary = f"{{metadata_file}}.{{os.getpid()}}.tmp"
with open(temporary, "w", encoding="utf-8") as handle:
    json.dump({{
        "owner": "clio-relay",
        "session_id": {session_id!r},
        "remote_frpc_pid": pid_value,
        "remote_frpc_pgid": process_group_id,
        "remote_frpc_config": config_file,
        "remote_frpc_log": log_file,
        "owner_token": owner_token,
        "connector_generation_id": generation_id,
        "process_start_ticks": process_start_ticks,
        "started_at": datetime.now(timezone.utc).isoformat(),
    }}, handle, indent=2)
os.chmod(temporary, 0o600)
os.replace(temporary, metadata_file)
__CLIO_METADATA__
sleep 1
if ! kill -0 "$pid" 2>/dev/null; then
  cat "$log_file" >&2
  exit 1
fi
start_complete=1
trap - EXIT
echo "remote_frpc_pid=$pid"
echo "remote_frpc_config=$config_file"
echo "remote_frpc_log=$log_file"
echo "remote_frpc_pgid=$pid"
echo "connector_generation_id=$connector_generation_id"
"""


def remote_stop_script(*, session_id: str, pid: int) -> str:
    return f"""set -euo pipefail
session_id={shlex.quote(session_id)}
pid={pid}
session_dir="$HOME/.local/share/clio-relay/service-sessions/$session_id"
metadata_file="$session_dir/metadata.json"
pid_file="$session_dir/remote-frpc.pid"
mkdir -p "$session_dir"
exec 9>"$session_dir/transition.lock"
flock -w 10 -x 9 || {{ echo "connector stop lock timed out" >&2; exit 75; }}
python3 - "$metadata_file" "$pid_file" "$pid" "$session_id" <<'__CLIO_STOP_CONNECTOR__'
import ctypes
import errno
import json
import os
import platform
import signal
import sys
import time
from pathlib import Path

metadata_file, pid_file, pid_raw, session_id = sys.argv[1:]
pid = int(pid_raw)
try:
    with open(metadata_file, encoding="utf-8") as handle:
        metadata = json.load(handle)
except (FileNotFoundError, json.JSONDecodeError) as exc:
    raise RuntimeError("durable connector ownership metadata is unavailable") from exc
if metadata.get("owner") != "clio-relay" or metadata.get("session_id") != session_id:
    raise RuntimeError("connector metadata owner/session mismatch")
if metadata.get("remote_frpc_pid") != pid:
    raise RuntimeError("connector pid does not match metadata")

token = metadata.get("owner_token")
generation_id = metadata.get("connector_generation_id")
pgid = metadata.get("remote_frpc_pgid")
durable_identity = (
    isinstance(token, str) and bool(token)
    and isinstance(generation_id, str) and bool(generation_id)
    and isinstance(pgid, int)
    and isinstance(metadata.get("process_start_ticks"), str)
    and isinstance(metadata.get("remote_frpc_config"), str)
)
if not durable_identity:
    raise RuntimeError("durable connector ownership identity is incomplete")


def owned_group_processes():
    token_marker = f"CLIO_RELAY_CONNECTOR_OWNER_TOKEN={{token}}".encode()
    generation_marker = f"CLIO_RELAY_CONNECTOR_GENERATION_ID={{generation_id}}".encode()
    matches = []
    for proc in Path("/proc").iterdir():
        if not proc.name.isdigit():
            continue
        member_pid = int(proc.name)
        try:
            if proc.stat().st_uid != os.geteuid():
                continue
            fields = (proc / "stat").read_text(encoding="utf-8").rsplit(")", 1)[1].split()
        except (FileNotFoundError, ProcessLookupError):
            continue
        except (OSError, IndexError, ValueError) as exc:
            raise RuntimeError(
                f"cannot inspect remote connector candidate {{member_pid}}: {{exc}}"
            ) from exc
        if fields[0] == "Z":
            continue
        try:
            environment = (proc / "environ").read_bytes().split(bytes([0]))
        except (FileNotFoundError, ProcessLookupError):
            continue
        except OSError as exc:
            raise RuntimeError(
                f"cannot verify remote connector candidate {{member_pid}}: {{exc}}"
            ) from exc
        if (
            token_marker in environment
            and generation_marker in environment
        ):
            matches.append(member_pid)
    return sorted(matches)


def open_process_fd(member_pid):
    native_open = getattr(os, "pidfd_open", None)
    if callable(native_open):
        return native_open(member_pid, 0)
    library = ctypes.CDLL(None, use_errno=True)
    libc_open = getattr(library, "pidfd_open", None)
    ctypes.set_errno(0)
    if libc_open is not None:
        libc_open.argtypes = [ctypes.c_int, ctypes.c_uint]
        libc_open.restype = ctypes.c_int
        descriptor = libc_open(member_pid, 0)
    else:
        if platform.machine().lower() not in ("aarch64", "amd64", "arm64", "x86_64"):
            raise RuntimeError("raw pidfd_open syscall ABI is unavailable")
        syscall = library.syscall
        syscall.restype = ctypes.c_long
        descriptor = syscall(
            ctypes.c_long({_LINUX_PIDFD_OPEN_SYSCALL_NUMBER}),
            ctypes.c_int(member_pid),
            ctypes.c_uint(0),
        )
    if descriptor < 0:
        error = ctypes.get_errno() or errno.ENOSYS
        raise OSError(error, os.strerror(error))
    return descriptor


def send_process_fd_signal(process_fd, sig):
    native_send = getattr(signal, "pidfd_send_signal", None)
    if callable(native_send):
        native_send(process_fd, sig, None, 0)
        return
    library = ctypes.CDLL(None, use_errno=True)
    libc_send = getattr(library, "pidfd_send_signal", None)
    ctypes.set_errno(0)
    if libc_send is not None:
        libc_send.argtypes = [
            ctypes.c_int,
            ctypes.c_int,
            ctypes.c_void_p,
            ctypes.c_uint,
        ]
        libc_send.restype = ctypes.c_int
        result = libc_send(process_fd, sig, None, 0)
    else:
        if platform.machine().lower() not in ("aarch64", "amd64", "arm64", "x86_64"):
            raise RuntimeError("raw pidfd_send_signal syscall ABI is unavailable")
        syscall = library.syscall
        syscall.restype = ctypes.c_long
        result = syscall(
            ctypes.c_long({_LINUX_PIDFD_SEND_SIGNAL_SYSCALL_NUMBER}),
            ctypes.c_int(process_fd),
            ctypes.c_int(sig),
            ctypes.c_void_p(),
            ctypes.c_uint(0),
        )
    if result < 0:
        error = ctypes.get_errno() or errno.ENOSYS
        raise OSError(error, os.strerror(error))


def signal_owned_processes(sig):
    signaled = []
    for member_pid in owned_group_processes():
        try:
            process_fd = open_process_fd(member_pid)
        except ProcessLookupError:
            continue
        except OSError as exc:
            raise RuntimeError(f"cannot open connector pidfd for {{member_pid}}: {{exc}}") from exc
        try:
            if member_pid not in owned_group_processes():
                continue
            try:
                send_process_fd_signal(process_fd, sig)
            except ProcessLookupError:
                continue
            except OSError as exc:
                raise RuntimeError(
                    f"cannot signal owned connector pid {{member_pid}}: {{exc}}"
                ) from exc
            signaled.append(member_pid)
        finally:
            os.close(process_fd)
    return signaled


proc = Path("/proc") / str(pid)
matches = owned_group_processes()
if proc.exists():
    try:
        command = (proc / "cmdline").read_bytes().replace(bytes([0]), b" ").decode(
            "utf-8", errors="replace"
        )
        environment = (proc / "environ").read_bytes().split(bytes([0]))
        fields = (proc / "stat").read_text(encoding="utf-8").rsplit(")", 1)[1].split()
        leader_owned = (
            fields[0] != "Z"
            and fields[19] == metadata["process_start_ticks"]
            and os.getpgid(pid) == pgid
            and f"CLIO_RELAY_CONNECTOR_OWNER_TOKEN={{token}}".encode() in environment
            and f"CLIO_RELAY_CONNECTOR_GENERATION_ID={{generation_id}}".encode()
            in environment
            and "frpc" in command
            and metadata["remote_frpc_config"] in command
        )
    except FileNotFoundError:
        leader_owned = False
    except (OSError, IndexError) as exc:
        raise RuntimeError(f"connector leader ownership observation failed: {{exc}}") from exc
    if not leader_owned and not matches:
        raise RuntimeError("connector leader PID ownership proof failed")
if not matches:
    Path(pid_file).unlink(missing_ok=True)
    print(json.dumps({{
        "pid": pid,
        "outcome": "missing",
        "ownership_verified": True,
        "verified_after_operation": True,
        "residual": False,
        "remaining_pids": [],
    }}))
    raise SystemExit(0)

signal_owned_processes(signal.SIGTERM)
for _ in range(25):
    if not owned_group_processes():
        break
    time.sleep(0.2)
remaining = owned_group_processes()
if remaining:
    signal_owned_processes(signal.SIGKILL)
    time.sleep(0.2)
remaining = owned_group_processes()
if remaining:
    raise RuntimeError("connector process group remains after SIGKILL")
Path(pid_file).unlink(missing_ok=True)
print(json.dumps({{
    "pid": pid,
    "outcome": "stopped",
    "ownership_verified": True,
    "verified_after_operation": True,
    "residual": False,
    "remaining_pids": remaining,
}}))
__CLIO_STOP_CONNECTOR__
"""
