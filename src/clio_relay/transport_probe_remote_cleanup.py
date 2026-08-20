"""Remote FRP transport-probe cleanup verification.

Split out of ``transport_probe.py`` (iowarp/clio-relay#231): stopping only
token-verified remote probe process groups (never a bare ``pkill``) and
turning the result into structured transport-probe evidence.
``_cleanup_remote_probe`` is itself a monkeypatch seam --
``tests/test_transport_probe.py`` patches
``clio_relay.transport_probe._cleanup_remote_probe`` and expects
``_finish_frp_probe_cleanup`` (still resident in ``transport_probe.py``, see
that module's docstring) to see the fake -- so only the *caller* needs to
stay resident; this module's real implementation is imported back into
``transport_probe.py`` by the same name and resolved there as a bare name at
call time, exactly like ``_wait_for_healthz`` in
``transport_probe_primitives.py``.
"""

from __future__ import annotations

import json
import subprocess
from typing import cast

from clio_relay.cluster_config import ClusterDefinition
from clio_relay.errors import RelayError
from clio_relay.transport_probe_evidence import (
    _attach_transport_evidence,
    _process_cleanup_resource,
    _transport_resource_line,
)
from clio_relay.transport_probe_primitives import _shell_single_quote
from clio_relay.transport_probe_remote_cleanup_models import (
    MAX_REMOTE_CLEANUP_OUTPUT_BYTES,
    _RemoteCleanupPayload,
)
from clio_relay.validation_report import TransportCleanupResourceEvidence

REMOTE_PROBE_CLEANUP_TIMEOUT_SECONDS = 120.0


def _reject_nonfinite_json_constant(value: str) -> None:
    raise ValueError(f"non-finite JSON constant is forbidden: {value}")


def _last_json_line(output: str) -> dict[str, object]:
    """Return the last JSON object emitted by a cleanup command."""
    if len(output.encode("utf-8")) > MAX_REMOTE_CLEANUP_OUTPUT_BYTES:
        raise RelayError("remote cleanup output exceeded the bounded size")
    for line in reversed(output.splitlines()):
        try:
            value = cast(
                object,
                json.loads(line, parse_constant=_reject_nonfinite_json_constant),
            )
        except (json.JSONDecodeError, RecursionError, ValueError):
            continue
        if isinstance(value, dict):
            return cast(dict[str, object], value)
    raise RelayError("remote cleanup did not emit a bounded JSON object")


def _cleanup_remote_probe(
    *,
    definition: ClusterDefinition,
    probe_id: str,
    require_metadata: bool = True,
    cluster: str | None = None,
) -> list[str]:
    """Stop only token-verified remote probe groups and return cleanup evidence."""
    script = f"""set -euo pipefail
probe_id={_shell_single_quote(probe_id)}
probe_dir="$HOME/.local/share/clio-relay/transport-probes/$probe_id"
metadata_file="$probe_dir/metadata.json"
for _ in 1 2 3 4 5 6 7 8 9 10; do
  [ -f "$metadata_file" ] && break
  sleep 0.5
done
[ -f "$metadata_file" ] || {{
  echo '{{"outcome":"metadata_missing","resources":[],"residual_processes":[]}}'
  exit {2 if require_metadata else 0}
}}
python3 - "$metadata_file" "$probe_id" <<'__CLIO_RELAY_CLEANUP_PROBE__'
import json
import os
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

metadata_path = Path(sys.argv[1])
expected_probe_id = sys.argv[2]
try:
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
except (OSError, json.JSONDecodeError) as exc:
    print(json.dumps({{"outcome": "invalid_metadata", "error": str(exc)}}))
    raise SystemExit(2)
if (
    metadata.get("owner") != "clio-relay"
    or metadata.get("probe_id") != expected_probe_id
):
    print(json.dumps({{"outcome": "ownership_refused", "error": "metadata owner/probe mismatch"}}))
    raise SystemExit(2)
token = metadata.get("owner_token")
processes = metadata.get("processes")
if not isinstance(token, str) or not token or not isinstance(processes, list):
    print(json.dumps({{
        "outcome": "invalid_metadata",
        "error": "missing owner token/process records",
    }}))
    raise SystemExit(2)

def process_info(pid):
    proc = Path("/proc") / str(pid)
    try:
        command = (proc / "cmdline").read_bytes().replace(bytes([0]), b" ").decode(
            "utf-8", errors="replace"
        )
        environment = (proc / "environ").read_bytes().split(bytes([0]))
        stat_fields = (proc / "stat").read_text(encoding="utf-8").rsplit(")", 1)[1].split()
        pgid = os.getpgid(pid)
    except OSError:
        return None
    return {{
        "command": command,
        "environment": environment,
        "pgid": pgid,
        "state": stat_fields[0],
        "start_ticks": stat_fields[19],
    }}

def token_processes():
    matches = []
    needle = f"CLIO_RELAY_PROBE_OWNER_TOKEN={{token}}".encode()
    for proc in Path("/proc").iterdir():
        if not proc.name.isdigit():
            continue
        try:
            environment = (proc / "environ").read_bytes().split(bytes([0]))
            if needle not in environment:
                continue
            info = process_info(int(proc.name))
        except OSError:
            continue
        if info is not None and info["state"] != "Z":
            matches.append((int(proc.name), info))
    return matches

resources = []
authorized_groups = set()
recorded_groups = set()
errors = []
for raw in processes:
    if not isinstance(raw, dict):
        errors.append("process record is not an object")
        continue
    kind = raw.get("kind")
    pid = raw.get("pid")
    pgid = raw.get("pgid")
    start_ticks = raw.get("process_start_ticks")
    markers = raw.get("command_contains")
    if (
        not isinstance(kind, str)
        or not isinstance(pid, int)
        or not isinstance(pgid, int)
        or not isinstance(start_ticks, str)
        or not isinstance(markers, list)
        or not all(isinstance(marker, str) for marker in markers)
    ):
        errors.append(f"invalid process record: {{raw!r}}")
        continue
    recorded_groups.add(pgid)
    info = process_info(pid)
    if info is None or info["state"] == "Z":
        resources.append({{
            "kind": kind,
            "pid": pid,
            "outcome": "missing",
            "ownership_verified": True,
        }})
        continue
    if info["start_ticks"] != start_ticks:
        resources.append({{
            "kind": kind,
            "pid": pid,
            "outcome": "replaced",
            "ownership_verified": False,
        }})
        continue
    owned = (
        pgid == pid
        and info["pgid"] == pgid
        and f"CLIO_RELAY_PROBE_OWNER_TOKEN={{token}}".encode() in info["environment"]
        and all(marker in info["command"] for marker in markers)
    )
    if not owned:
        resources.append({{
            "kind": kind,
            "pid": pid,
            "outcome": "refused",
            "ownership_verified": False,
        }})
        errors.append(f"ownership proof failed for {{kind}} pid {{pid}}")
        continue
    authorized_groups.add(pgid)
    resources.append({{
        "kind": kind,
        "pid": pid,
        "outcome": "stopping",
        "ownership_verified": True,
    }})

for _pid, info in token_processes():
    if info["pgid"] in recorded_groups:
        authorized_groups.add(info["pgid"])
for pgid in sorted(authorized_groups):
    try:
        os.killpg(pgid, signal.SIGTERM)
    except ProcessLookupError:
        pass
deadline = time.monotonic() + 5
while time.monotonic() < deadline:
    if not token_processes():
        break
    time.sleep(0.2)
for pid, info in token_processes():
    pgid = info["pgid"]
    if pgid not in authorized_groups:
        errors.append(f"token-owned pid {{pid}} has unexpected process group {{pgid}}")
        continue
    try:
        os.killpg(pgid, signal.SIGKILL)
    except ProcessLookupError:
        pass
time.sleep(0.2)
residuals = []
for pid, info in token_processes():
    residuals.append({{"pid": pid, "pgid": info["pgid"], "state": info["state"]}})
for resource in resources:
    if resource["outcome"] == "stopping":
        resource["outcome"] = "stopped" if not residuals else "residual"
tmp = metadata.get("tmp")
if isinstance(tmp, str) and tmp.startswith("/tmp/"):
    import shutil
    shutil.rmtree(tmp, ignore_errors=True)
cleanup = {{
    "outcome": "passed" if not errors and not residuals else "failed",
    "completed_at": datetime.now(timezone.utc).isoformat(),
    "resources": resources,
    "residual_processes": residuals,
    "errors": errors,
}}
metadata["cleanup"] = cleanup
temporary = metadata_path.with_suffix(".tmp")
temporary.write_text(json.dumps(metadata, indent=2) + "\\n", encoding="utf-8")
os.replace(temporary, metadata_path)
print(json.dumps(cleanup, sort_keys=True))
if errors or residuals:
    raise SystemExit(2)
__CLIO_RELAY_CLEANUP_PROBE__
"""
    try:
        result = subprocess.run(
            ["ssh", definition.ssh_host, "bash", "-s"],
            input=script.encode("utf-8"),
            capture_output=True,
            check=False,
            timeout=REMOTE_PROBE_CLEANUP_TIMEOUT_SECONDS,
        )
    except (subprocess.TimeoutExpired, OSError) as exc:
        detail = (
            "remote cleanup command timed out after "
            f"{REMOTE_PROBE_CLEANUP_TIMEOUT_SECONDS:g} seconds"
            if isinstance(exc, subprocess.TimeoutExpired)
            else f"remote cleanup command could not start: {exc}"
        )
        evidence_line = _unverified_remote_cleanup_evidence_line(
            cluster=cluster or definition.name,
            definition=definition,
            probe_id=probe_id,
            detail=detail,
        )
        error = RelayError(f"remote transport cleanup failed for {probe_id}: {detail}")
        raise _attach_transport_evidence(error, [evidence_line]) from exc
    if (
        len(result.stdout) > MAX_REMOTE_CLEANUP_OUTPUT_BYTES
        or len(result.stderr) > MAX_REMOTE_CLEANUP_OUTPUT_BYTES
    ):
        evidence_line = _unverified_remote_cleanup_evidence_line(
            cluster=cluster or definition.name,
            definition=definition,
            probe_id=probe_id,
            detail="remote cleanup output exceeded the bounded size",
        )
        error = RelayError(f"remote transport cleanup output was too large for {probe_id}")
        raise _attach_transport_evidence(error, [evidence_line])
    stdout = result.stdout.decode("utf-8", errors="replace").strip()
    stderr = result.stderr.decode("utf-8", errors="replace").strip()
    try:
        cleanup = _RemoteCleanupPayload.model_validate(_last_json_line(stdout))
    except (ValueError, RelayError) as exc:
        evidence_line = _unverified_remote_cleanup_evidence_line(
            cluster=cluster or definition.name,
            definition=definition,
            probe_id=probe_id,
            detail=f"remote cleanup output was invalid: {exc}",
        )
        detail = stderr or stdout or str(exc)
        error = RelayError(f"remote transport cleanup failed for {probe_id}: {detail}")
        raise _attach_transport_evidence(error, [evidence_line]) from exc
    evidence_line = _remote_cleanup_evidence_line(
        cleanup,
        cluster=cluster or definition.name,
        definition=definition,
        probe_id=probe_id,
    )
    if cleanup.outcome == "metadata_missing" and not require_metadata:
        return [evidence_line, "transport.remote_cleanup=not_started"]
    if result.returncode != 0 or cleanup.outcome != "passed":
        detail = stderr or cleanup.error or "; ".join(cleanup.errors) or stdout
        error = RelayError(f"remote transport cleanup failed for {probe_id}: {detail}")
        raise _attach_transport_evidence(error, [evidence_line])
    resource_parts = [
        f"{resource.kind}:{resource.pid}:{resource.outcome}" for resource in cleanup.resources
    ]
    return [
        evidence_line,
        "transport.remote_cleanup=passed",
        f"transport.remote_cleanup_resources={','.join(resource_parts)}",
        f"transport.remote_cleanup_residuals={len(cleanup.residual_processes)}",
        (
            "transport.remote_cleanup_metadata="
            f"~/.local/share/clio-relay/transport-probes/{probe_id}/metadata.json"
        ),
    ]


def _remote_cleanup_evidence_line(
    cleanup: _RemoteCleanupPayload,
    *,
    cluster: str,
    definition: ClusterDefinition,
    probe_id: str,
) -> str:
    detail = cleanup.error or ("; ".join(cleanup.errors) if cleanup.errors else None)
    completed = cleanup.outcome in {"passed", "failed"}
    resources: list[TransportCleanupResourceEvidence] = [
        _process_cleanup_resource(
            kind="relay_session",
            resource_id=f"frp-probe:{probe_id}",
            role="remote_transport_probe_session",
            location=definition.ssh_host,
            ownership_verified=completed,
            outcome="stopped" if cleanup.outcome == "passed" else cleanup.outcome,
            verified_after_operation=cleanup.outcome == "passed",
            observed_state=("stopped" if cleanup.outcome == "passed" else "running_or_unknown"),
            residual=cleanup.outcome != "passed",
            detail=detail,
            metadata={"remote_cleanup_outcome": cleanup.outcome},
        )
    ]
    for resource in cleanup.resources:
        canonical_kind = "connector" if resource.kind == "remote_connector" else "relay_process"
        role = (
            "remote_frpc_connector"
            if resource.kind == "remote_connector"
            else "remote_relay_api_process"
        )
        residual = resource.outcome in {"refused", "residual"}
        verified = resource.outcome in {"missing", "replaced", "stopped"}
        resources.append(
            _process_cleanup_resource(
                kind=canonical_kind,
                resource_id=str(resource.pid),
                role=role,
                location=definition.ssh_host,
                ownership_verified=resource.ownership_verified,
                outcome=resource.outcome,
                verified_after_operation=verified,
                observed_state=resource.outcome,
                residual=residual,
                detail=(
                    f"cleanup {resource.outcome} for {resource.kind} pid {resource.pid}"
                    if resource.outcome not in {"missing", "stopped"}
                    else None
                ),
                metadata={
                    "cleanup_kind": resource.kind,
                    "pid": resource.pid,
                },
            )
        )
    existing = {(resource.kind, resource.resource_id) for resource in resources}
    for residual in cleanup.residual_processes:
        identity = ("relay_process", str(residual.pid))
        if identity in existing:
            continue
        existing.add(identity)
        resources.append(
            _process_cleanup_resource(
                kind="relay_process",
                resource_id=str(residual.pid),
                role="remote_probe_residual_process",
                location=definition.ssh_host,
                ownership_verified=True,
                outcome="residual",
                verified_after_operation=False,
                observed_state=residual.state,
                residual=True,
                detail=f"owned process group {residual.pgid} remained after cleanup",
                metadata={"pid": residual.pid, "pgid": residual.pgid},
            )
        )
    return _transport_resource_line(
        probe_id=probe_id,
        cluster=cluster,
        cleanup_mode="transport_probe_teardown",
        resources=resources,
    )


def _unverified_remote_cleanup_evidence_line(
    *,
    cluster: str,
    definition: ClusterDefinition,
    probe_id: str,
    detail: str,
) -> str:
    return _transport_resource_line(
        probe_id=probe_id,
        cluster=cluster,
        cleanup_mode="transport_probe_teardown",
        resources=[
            _process_cleanup_resource(
                kind="relay_session",
                resource_id=f"frp-probe:{probe_id}",
                role="remote_transport_probe_session",
                location=definition.ssh_host,
                ownership_verified=False,
                outcome="unknown",
                verified_after_operation=False,
                observed_state="running_or_unknown",
                residual=True,
                detail=detail,
            )
        ],
    )
