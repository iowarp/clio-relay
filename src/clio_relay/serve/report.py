"""What relay reads from a service's files on the site, and what it tells a person about it.

Everything here is pure: :mod:`clio_relay.serve.runtime` fetches the files and
passes their bytes in.
"""

from __future__ import annotations

import base64
import json
import re
from typing import Any, cast

from clio_relay.jobs.handles import JobHandle, JobStatus, OutputRef
from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.serve.plan import SERVE_DIR, ServePlan

LOG_TAIL = 4096
# frpc's log line: "2026-11-18 12:38:59.277 [W] [client/service.go:319] text".
_FRPC_LINE = re.compile(r"^\S+ \S+ \[([A-Z])\] \[[^\]]*\] (.*)$")
# The wrapper's own files. Logs stay, because a person reads them after a failure.
_SERVE_FILES = {"service.log", "launch.log", "frpc.log"}


def times(count: int) -> str:
    return "once" if count == 1 else f"{count} times"


def handle_job_dir(handle: JobHandle) -> str | None:
    """Return the job directory from a handle, looking inside one an adapter wraps.

    The worker adapter puts the handle its site-side adapter made under ``data["remote"]``.
    """
    data: dict[str, Any] | None = handle.data
    while data is not None:
        job_dir = data.get("job_dir")
        if job_dir:
            return str(job_dir)
        remote = data.get("remote")
        inner = cast(dict[str, Any], remote).get("data") if isinstance(remote, dict) else None
        data = cast(dict[str, Any], inner) if isinstance(inner, dict) else None
    return None


def job_path(plan: ServePlan, name: str) -> str:
    """Return the path of one of the wrapper's files in the job's ``serve/`` directory."""
    if not plan.job_dir:
        raise Refusal(
            RefusalReason.INVALID_CONFIG,
            f"adapter {plan.adapter} gave no job_dir in its handle; relay cannot read serve/",
        )
    return f"{plan.job_dir.rstrip('/')}/{SERVE_DIR}/{name}"


def redact(plan: ServePlan, text: str) -> str:
    for secret in [*plan.secrets.values(), plan.secret_key, plan.hop.token if plan.hop else ""]:
        if secret:
            text = text.replace(secret, "***")
    return text


def _lines(data: bytes) -> list[str]:
    return [line for line in data.decode(errors="replace").splitlines() if line.strip()]


def last_line(plan: ServePlan, data: bytes | None) -> str | None:
    """Return the last line of the service's log, with its secrets hidden."""
    lines = _lines(data) if data else []
    return redact(plan, lines[-1][-300:]) if lines else None


def frpc_problem(plan: ServePlan, data: bytes | None) -> str | None:
    """Return frpc's latest warning or error on the site, unless it logged in after it."""
    if plan.mode != "frp" or not data:
        return None
    for line in reversed(_lines(data)):
        if "login to server success" in line:
            return None
        match = _FRPC_LINE.match(line)
        if match is not None and match.group(1) in ("W", "E"):
            return redact(plan, match.group(2).strip()[-300:])
    return None


def final_message(plan: ServePlan, status: JobStatus, stopped: dict[str, Any]) -> str:
    """Say how the service ended, from the wrapper's ``stopped.json`` and the job's state."""
    reason = str(stopped.get("reason") or "")
    parts: list[str] = []
    if reason == "stopped":
        parts.append("stopped")
    elif reason == "idle":
        parts.append("released after its idle time")
    elif reason == "port_taken":
        node = stopped.get("host") or plan.host or "the node"
        port = stopped.get("port")
        named = f"port {port} ({stopped.get('port_name')})" if port else "a port it needs"
        parts.append(f"refused: {named} was already in use on {node}")
    elif reason == "exited":
        parts.append(f"the service exited with {stopped.get('exit_code')}")
    elif reason == "signal":
        parts.append("the job was signalled")
    elif status.message:
        parts.append(redact(plan, status.message))
    else:
        parts.append(f"the job is {status.state.value}")
    if plan.cancel_sent:
        parts.append("its job was cancelled")
    restarts = stopped.get("frpc_restarts")
    if restarts:
        parts.append(f"the site's frp client restarted {times(int(restarts))}")
    if plan.rebinds:
        parts.append(f"the desktop port was bound again {times(plan.rebinds)}")
    return "; ".join(parts)


def is_artifact(item: OutputRef) -> bool:
    """Return whether an adapter-listed output is the service's, not the wrapper's."""
    parts = item.name.replace("\\", "/").split("/")
    if len(parts) == 2 and parts[0] == "serve":
        return parts[1] in _SERVE_FILES
    return True


def listed_artifacts(plan: ServePlan, listing: bytes | None, seen: set[str]) -> list[OutputRef]:
    """Return the outputs in the wrapper's ``artifacts.txt`` that are not in ``seen`` yet."""
    outputs: list[OutputRef] = []
    if not listing or not plan.job_dir:
        return outputs
    for line in listing.decode(errors="replace").splitlines():
        size_text, _, name = line.partition("\t")
        name = name.strip()
        if not name:
            continue
        path = f"{plan.job_dir.rstrip('/')}/{name}"
        if path in seen:
            continue
        seen.add(path)
        size = int(size_text) if size_text.strip().isdigit() else None
        outputs.append(OutputRef(name=name, path=path, size=size))
    return outputs


def connect_handle(plan: ServePlan) -> str:
    """Encode what a desktop needs to reach the service through the rendezvous host.

    It carries the proxies' shared key, not the rendezvous token: the desktop
    reads the token from its own rendezvous setting.
    """
    rendezvous = plan.rendezvous
    payload = {
        "v": 1,
        "op": plan.operation_id,
        "sk": plan.secret_key,
        "direct": bool(rendezvous and rendezvous.direct),
        "ports": [[port.name, port.protocol, port.local_port] for port in plan.ports],
    }
    raw = json.dumps(payload, separators=(",", ":")).encode()
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")
