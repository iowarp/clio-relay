"""Small helpers of the serve runtime (:mod:`clio_relay.serve.runtime`)."""

from __future__ import annotations

import inspect
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from clio_relay.serve.plan import ServePlan

HOST_KEYS_SH = (
    'for f in /etc/ssh/ssh_known_hosts /etc/ssh/ssh_known_hosts2 "$HOME/.ssh/known_hosts"; '
    'do [ -r "$f" ] && ssh-keygen -F "$1" -f "$f"; done; exit 0'
)
"""Print a host's lines from the site's known_hosts files; ``$1`` is the host."""


def safe_name(text: str) -> str:
    """Return ``text`` with only letters, digits and ``._-``, for a file name."""
    return "".join(char if char.isalnum() or char in "._-" else "_" for char in text) or "site"


def scheduler_job_name(operation_id: str) -> str:
    """The batch job name relay's adapters give an operation; the hop finds a queued job by it."""
    return f"relay-{operation_id}"


def state_dir_of(adapter: Any) -> Path:
    """Return relay's state directory from an adapter's per-site one."""
    state_dir = Path(adapter.context.state_dir)
    if state_dir.parent.parent.name == "sites":
        return state_dir.parent.parent.parent
    return state_dir


async def maybe(value: Any) -> Any:
    """Await ``value`` when it is awaitable."""
    return await value if inspect.isawaitable(value) else value


def site_id_of(site: Any) -> str:
    """Return a site's id, given the site or its id."""
    return site if isinstance(site, str) else str(site.id)


def tunnel_entry(operation_id: str, plan: ServePlan, pid: int | None) -> dict[str, Any]:
    """Return shutdown's entry for a service's desktop ports."""
    entry: dict[str, Any] = {
        "kind": "tunnel",
        "detail": f"desktop ports of service {operation_id} ({plan.mode})",
    }
    if pid:
        entry["pid"] = pid
    return entry
