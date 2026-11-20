"""Stop every long-lived process relay started, for one site or all of them.

A host calls this when it closes, so that nothing relay started outlives it: the
worker's site-serve, the login hop, the desktop forwards (frpc visitors and
``ssh -L``), ``clio-relay connect`` connectors, the ssh control connection, and a
detached ``clio-relay serve --http``. Jobs are never cancelled: each one still
running is listed with ``kind: job``. A site relay cannot reach is a failure in the
result, never an exception. The result's schema is ``relay.shutdown.v1``.
"""

from __future__ import annotations

import asyncio
import atexit
import contextlib
import json
import os
import signal
import subprocess
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any

from clio_relay.adapters.base import ShutsDown
from clio_relay.config import RelaySettings
from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.sites.models import SiteEntry
from clio_relay.surface.settings import RelayPaths

if TYPE_CHECKING:
    from clio_relay.surface.service import RelayService

SCHEMA = "relay.shutdown.v1"
DEFAULT_TIMEOUT_S = 15.0
MAX_TIMEOUT_S = 120.0
LOCAL = "local"
_JOB_KINDS = ("run", "serve")
_SELF_EXIT_DELAY_S = 1.0


def servers_dir(paths: RelayPaths) -> Path:
    """Return where each ``clio-relay serve --http`` records itself."""
    return paths.state_dir / "servers"


def register_server(paths: RelayPaths, url: str) -> Path:
    """Record this process as a relay HTTP server until it exits."""
    directory = servers_dir(paths)
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{os.getpid()}.json"
    path.write_text(json.dumps({"pid": os.getpid(), "url": url}), encoding="utf-8")

    def forget() -> None:
        with contextlib.suppress(OSError):
            path.unlink()

    atexit.register(forget)
    return path


def _cmdline(pid: int) -> str | None:
    if sys.platform.startswith("linux"):
        try:
            raw = Path(f"/proc/{pid}/cmdline").read_bytes()
        except OSError:
            return None
        return raw.replace(b"\0", b" ").decode(errors="replace")
    if os.name == "nt":
        result = subprocess.run(
            ["tasklist", "/FI", f"PID eq {pid}", "/FO", "CSV", "/NH"],
            capture_output=True,
            text=True,
            check=False,
        )
        return result.stdout if str(pid) in result.stdout else None
    try:
        result = subprocess.run(
            ["ps", "-o", "command=", "-p", str(pid)], capture_output=True, text=True, check=False
        )
    except OSError:
        return None
    return result.stdout.strip() or None


def _is_relay(pid: int, word: str) -> bool:
    """Whether ``pid`` is a relay process running ``word`` (Windows shows only the image)."""
    line = _cmdline(pid)
    if line is None:
        return False
    if os.name == "nt":
        return any(name in line.lower() for name in ("python", "clio-relay", "clio_relay"))
    return ("clio-relay" in line or "clio_relay" in line) and word in line


def _terminate(pid: int) -> bool:
    try:
        if os.name == "nt":
            subprocess.run(["taskkill", "/PID", str(pid), "/F"], capture_output=True, check=False)
        else:
            os.kill(pid, signal.SIGTERM)
    except OSError:
        return False
    return True


def _read_pid(path: Path) -> int | None:
    try:
        text = path.read_text(encoding="utf-8").strip()
    except OSError:
        return None
    if text.startswith("{"):
        with contextlib.suppress(ValueError):
            value = json.loads(text).get("pid")
            return value if isinstance(value, int) else None
        return None
    return int(text) if text.isdigit() else None


def _stop_connectors(paths: RelayPaths, operations: dict[str, str], site: str | None) -> list[Any]:
    """Stop ``clio-relay connect`` holders: all of them, or those of ``site``'s operations."""
    from clio_relay.serve.desktop import CONNECTOR_PIDFILE

    stopped: list[dict[str, Any]] = []
    for pidfile in sorted((paths.state_dir / "serve" / "connect").glob(f"*/{CONNECTOR_PIDFILE}")):
        operation = pidfile.parent.name
        owner = operations.get(operation, LOCAL)
        if site is not None and owner != site:
            continue
        pid = _read_pid(pidfile)
        if pid is None or pid == os.getpid() or not _is_relay(pid, "connect"):
            with contextlib.suppress(OSError):
                pidfile.unlink()
            continue
        if _terminate(pid):
            with contextlib.suppress(OSError):
                pidfile.unlink()
            stopped.append(
                {
                    "site": owner,
                    "kind": "connector",
                    "pid": pid,
                    "detail": f"clio-relay connect for service {operation}",
                }
            )
    return stopped


def _stop_servers(paths: RelayPaths) -> list[dict[str, Any]]:
    """Stop recorded ``serve --http`` processes; this one exits after it answers."""
    stopped: list[dict[str, Any]] = []
    for record in sorted(servers_dir(paths).glob("*.json")):
        pid = _read_pid(record)
        if pid == os.getpid():
            stopped.append(
                {
                    "site": LOCAL,
                    "kind": "local_server",
                    "pid": pid,
                    "detail": "this relay server; it exits once it has answered",
                }
            )
            with contextlib.suppress(RuntimeError):
                asyncio.get_running_loop().call_later(_SELF_EXIT_DELAY_S, _exit_self)
            continue
        if pid is not None and _is_relay(pid, "serve") and _terminate(pid):
            stopped.append(
                {
                    "site": LOCAL,
                    "kind": "local_server",
                    "pid": pid,
                    "detail": "a detached clio-relay serve --http",
                }
            )
        with contextlib.suppress(OSError):
            record.unlink()
    return stopped


def _exit_self() -> None:
    if os.name == "nt":
        os._exit(0)
    os.kill(os.getpid(), signal.SIGTERM)


def _reap_visitors() -> list[dict[str, Any]]:
    from clio_relay.frp_visitor_reconciliation import reconcile_stale_frp_visitors

    result = reconcile_stale_frp_visitors(frpc_bin=RelaySettings.from_env().frpc_bin)
    return [
        {"site": LOCAL, "kind": "frpc", "pid": pid, "detail": "an orphaned frpc visitor"}
        for pid in result.reaped_pids
    ]


async def _site(
    service: RelayService,
    entry: SiteEntry,
    stopped: list[dict[str, Any]],
    failures: list[dict[str, Any]],
    spare: frozenset[str] = frozenset(),
) -> None:
    """Stop ``entry``'s processes: desktop ports and the hop first, then the adapters'.

    Adapters named in ``spare`` keep their connection; ``site clean`` stops it last.
    """

    def failed(error: str, detail: str) -> None:
        failures.append({"site": entry.id, "error": error, "detail": detail})

    runtime: Any = None
    try:
        from clio_relay.serve import runtime as serve_runtime

        runtime = serve_runtime.current() or serve_runtime.ServeRuntime(service.paths.state_dir)
    except ModuleNotFoundError:
        runtime = None
    if runtime is not None:
        hop_adapter: Any = None
        with contextlib.suppress(Refusal):
            hop_adapter = service.adapter_for(entry, "exec")
        try:
            found = await runtime.shutdown_site(entry.id, hop_adapter)
            stopped.extend({"site": entry.id, **item} for item in found)
        except Refusal as refusal:
            failed(refusal.reason.value, refusal.message)
    adapters = [(entry.adapter, entry.config), *entry.alternatives.items()]
    for name, _config in adapters:
        if name in spare:
            continue
        try:
            adapter = service.adapter_named(entry, name)
        except Refusal:
            continue
        if not isinstance(adapter, ShutsDown):
            continue
        try:
            found = await adapter.shutdown()
        except Refusal as refusal:
            failed(refusal.reason.value, f"{name}: {refusal.message}")
            continue
        except Exception as exc:  # an unreachable site is a failure, never a raise
            failed(RefusalReason.UNREACHABLE.value, f"{name}: {exc}")
            continue
        stopped.extend({"site": entry.id, **item} for item in found)


def _still_running(service: RelayService, site_ids: set[str], hop_stopped: set[str]) -> list[Any]:
    running: list[dict[str, Any]] = []
    for record in service.operations.all():
        if record.site not in site_ids or record.kind not in _JOB_KINDS or record.state.final:
            continue
        detail = f"{record.kind} {record.state.value}; shutdown does not cancel jobs"
        if record.kind == "serve":
            detail += ". Its desktop ports are closed"
            if record.site in hop_stopped:
                detail += ", and its ports lost the login hop"
        running.append(
            {"site": record.site, "kind": "job", "id": record.operation_id, "detail": detail}
        )
    return running


def _check(arguments: dict[str, Any]) -> tuple[str | None, float]:
    site = arguments.get("site")
    every = arguments.get("all")
    if every not in (None, False, True):
        raise Refusal(RefusalReason.INVALID_ARGUMENTS, "all is true or left out")
    if (site is None) == (not every):
        raise Refusal(
            RefusalReason.INVALID_ARGUMENTS, 'give exactly one of {"all": true} or {"site": name}'
        )
    if site is not None and (not isinstance(site, str) or not site):
        raise Refusal(RefusalReason.INVALID_ARGUMENTS, "site is a site name")
    raw = arguments.get("timeout_s")
    timeout_s = DEFAULT_TIMEOUT_S if raw is None else raw
    if isinstance(timeout_s, bool) or not isinstance(timeout_s, int | float):
        raise Refusal(RefusalReason.INVALID_ARGUMENTS, "timeout_s is a number of seconds")
    if not 0 < timeout_s <= MAX_TIMEOUT_S:
        raise Refusal(
            RefusalReason.INVALID_ARGUMENTS, f"timeout_s is above 0 and at most {MAX_TIMEOUT_S:g}"
        )
    return site, float(timeout_s)


async def shutdown(
    service: RelayService, arguments: dict[str, Any], *, spare: frozenset[str] = frozenset()
) -> dict[str, Any]:
    """Stop relay's long-lived processes; see the module docstring.

    ``arguments`` is ``{"all": true}`` or ``{"site": name}``, with an optional
    ``timeout_s`` (default 15) for each site. Sites run in parallel. Bad arguments
    or an unknown site refuse; anything else is reported in the result. ``spare``
    names adapters whose own connection stays up, for ``site clean``.
    """
    site, timeout_s = _check(arguments)
    entries = [service.site(site)] if site is not None else service.sites.list()
    stopped: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []

    async def one(entry: SiteEntry) -> None:
        mine: list[dict[str, Any]] = []
        errors: list[dict[str, Any]] = []
        try:
            await asyncio.wait_for(_site(service, entry, mine, errors, spare), timeout_s)
        except TimeoutError:
            errors.append(
                {
                    "site": entry.id,
                    "error": RefusalReason.TIMEOUT.value,
                    "detail": f"the site did not finish shutting down in {timeout_s:g} s",
                }
            )
        except Exception as exc:
            errors.append(
                {"site": entry.id, "error": RefusalReason.UNREACHABLE.value, "detail": str(exc)}
            )
        stopped.extend(mine)
        failures.extend(errors)

    await asyncio.gather(*(one(entry) for entry in entries))
    operations = {record.operation_id: record.site for record in service.operations.all()}
    stopped.extend(_stop_connectors(service.paths, operations, site))
    if site is None:
        try:
            stopped.extend(_reap_visitors())
        except Exception as exc:
            failures.append(
                {"site": LOCAL, "error": RefusalReason.UNREACHABLE.value, "detail": str(exc)}
            )
        stopped.extend(_stop_servers(service.paths))
    hop_stopped = {item["site"] for item in stopped if item["kind"] == "hop_proxy"}
    return {
        "schema": SCHEMA,
        "stopped": stopped,
        "still_running": _still_running(service, {entry.id for entry in entries}, hop_stopped),
        "failures": failures,
    }
