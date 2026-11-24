"""Stop every long-lived process relay started, for one site or all of them.

A host calls this when it closes, so that nothing relay started outlives it: the
worker's site-serve, the login hop, the desktop forwards (relay's frp visitors and
``ssh -L``), ``clio-relay connect`` connectors, the ssh control connection, and a
detached ``clio-relay serve --http``. Jobs are never cancelled: each one still
running is listed with ``kind: job``. A site relay cannot reach is a failure in the
result, never an exception. The result's schema is ``relay.shutdown.v1``.
"""

from __future__ import annotations

import asyncio
import atexit
import contextlib
import hmac
import json
import os
import secrets
import signal
import ssl
import subprocess
import sys
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, cast

from clio_relay.adapters.base import ShutsDown
from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.sites.models import SiteEntry
from clio_relay.surface.settings import RelayPaths

if TYPE_CHECKING:
    from clio_relay.surface.service import RelayService

SCHEMA = "relay.shutdown.v1"
DEFAULT_TIMEOUT_S = 15.0
MAX_TIMEOUT_S = 120.0
LOCAL = "local"
_JOB_KINDS = ("run", "serve", "install")
_SELF_EXIT_DELAY_S = 1.0
_HTTP_STOP_TIMEOUT_S = 5.0
STOP_ROUTE = "/relay/stop"
_QUERY_LIMITED_INFORMATION = 0x1000
_ERROR_INVALID_PARAMETER = 87
_STILL_ACTIVE = 259

Probe = Literal["relay", "other", "gone", "unknown"]


def servers_dir(paths: RelayPaths) -> Path:
    """Return where each ``clio-relay serve --http`` records itself."""
    return paths.state_dir / "servers"


def register_server(
    paths: RelayPaths, server: Any, url: str, origin: str, *, tls_cert: Path | None = None
) -> Path:
    """Record this process as a relay HTTP server until it exits, and mount its stop route.

    The record is private (0600) and holds a stop secret made for this process
    alone, separate from the MCP bearer token. A host that cannot see the process
    (another identity, or a sandbox) can still ask it to exit: ``POST``
    :data:`STOP_ROUTE` with that secret as a bearer token. Any other request there
    gets 401, so the route never stops a server without the secret.
    """
    from starlette.requests import Request
    from starlette.responses import Response

    directory = servers_dir(paths)
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{os.getpid()}.json"
    secret = secrets.token_urlsafe(32)
    record: dict[str, Any] = {
        "pid": os.getpid(),
        "url": url,
        "origin": origin,
        "stop_token": secret,
    }
    if tls_cert is not None:
        record["tls_cert"] = str(tls_cert.resolve())
    with contextlib.suppress(FileNotFoundError):
        path.unlink()
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        json.dump(record, handle)

    def forget() -> None:
        with contextlib.suppress(OSError):
            path.unlink()

    async def stop(request: Request) -> Response:
        scheme, _, given = (request.headers.get("authorization") or "").partition(" ")
        if scheme.lower() != "bearer" or not hmac.compare_digest(
            given.strip().encode(), secret.encode()
        ):
            return Response(status_code=401, headers={"WWW-Authenticate": "Bearer"})
        forget()
        asyncio.get_running_loop().call_later(_SELF_EXIT_DELAY_S, _exit_self)
        return Response(status_code=202)

    server.custom_route(STOP_ROUTE, methods=["POST"], name="relay-stop", include_in_schema=False)(
        stop
    )
    atexit.register(forget)
    return path


def _cmdline(pid: int) -> str | None:
    if sys.platform.startswith("linux"):
        try:
            raw = Path(f"/proc/{pid}/cmdline").read_bytes()
        except OSError:
            return None
        return raw.replace(b"\0", b" ").decode(errors="replace")
    try:
        result = subprocess.run(
            ["ps", "-o", "command=", "-p", str(pid)], capture_output=True, text=True, check=False
        )
    except OSError:
        return None
    return result.stdout.strip() or None


def _probe_windows(pid: int) -> tuple[Probe, str]:
    """Probe ``pid`` through the process handle; ``tasklist`` hides other identities."""
    if sys.platform != "win32":
        return "unknown", "no Windows process API here"
    import ctypes
    from ctypes import wintypes

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.OpenProcess.restype = wintypes.HANDLE
    kernel32.OpenProcess.argtypes = (wintypes.DWORD, wintypes.BOOL, wintypes.DWORD)
    handle = kernel32.OpenProcess(_QUERY_LIMITED_INFORMATION, False, pid)
    if not handle:
        error = ctypes.get_last_error()
        if error == _ERROR_INVALID_PARAMETER:
            return "gone", ""
        return "unknown", (
            f"process {pid} cannot be opened (Windows error {error}); it may run as another "
            "user or outside this sandbox, so relay cannot confirm or stop it"
        )
    try:
        code = wintypes.DWORD()
        if kernel32.GetExitCodeProcess(handle, ctypes.byref(code)) and code.value != _STILL_ACTIVE:
            return "gone", ""
        size = wintypes.DWORD(32768)
        buffer = ctypes.create_unicode_buffer(size.value)
        if not kernel32.QueryFullProcessImageNameW(handle, 0, buffer, ctypes.byref(size)):
            return "unknown", (
                f"process {pid} is running, but its program cannot be read "
                f"(Windows error {ctypes.get_last_error()})"
            )
        image = Path(buffer.value).name.lower()
    finally:
        kernel32.CloseHandle(handle)
    if any(name in image for name in ("python", "clio-relay", "clio_relay")):
        return "relay", ""
    return "other", ""


def _probe(pid: int, word: str) -> tuple[Probe, str]:
    """Whether ``pid`` is relay running ``word``, another program, gone, or unknown, and why.

    Windows shows only the program's image, so ``word`` is not checked there.
    """
    if os.name == "nt":
        return _probe_windows(pid)
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return "gone", ""
    except PermissionError:
        return "unknown", f"process {pid} belongs to another user; relay cannot confirm or stop it"
    except OSError as exc:
        return "unknown", f"process {pid} cannot be checked: {exc}"
    line = _cmdline(pid)
    if line is None:
        return "unknown", f"process {pid} is running, but its command line cannot be read"
    if ("clio-relay" in line or "clio_relay" in line) and word in line:
        return "relay", ""
    return "other", ""


def _terminate(pid: int) -> str | None:
    """Stop ``pid``; return why it could not be stopped, or None once it is stopped."""
    if os.name == "nt":
        try:
            result = subprocess.run(
                ["taskkill", "/PID", str(pid), "/F"], capture_output=True, text=True, check=False
            )
        except OSError as exc:
            return f"taskkill could not run: {exc}"
        if result.returncode == 0 or _probe(pid, "")[0] == "gone":
            return None
        output = (result.stderr or result.stdout).strip()
        return f"taskkill exited {result.returncode}: {output or 'no output'}"
    try:
        os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        return None
    except OSError as exc:
        return f"SIGTERM failed: {exc}"
    return None


async def _http_stop(record: dict[str, Any]) -> str | None:
    """Ask a recorded server to exit through its stop route; return why that failed, or None."""
    origin = record.get("origin")
    token = record.get("stop_token")
    if not isinstance(origin, str) or not isinstance(token, str):
        return "its record has no stop route (a relay server from before the route existed)"
    import httpx

    cert = record.get("tls_cert")
    verify: ssl.SSLContext | bool = (
        ssl.create_default_context(cafile=cert) if isinstance(cert, str) else True
    )
    try:
        async with httpx.AsyncClient(
            timeout=_HTTP_STOP_TIMEOUT_S, verify=verify, trust_env=False
        ) as client:
            response = await client.post(
                f"{origin}{STOP_ROUTE}", headers={"Authorization": f"Bearer {token}"}
            )
    except (httpx.HTTPError, OSError) as exc:
        return f"its stop route at {origin} did not answer: {exc}"
    if response.status_code not in (200, 202):
        return f"its stop route at {origin} answered HTTP {response.status_code}"
    return None


async def _stop_record(
    record: Path,
    pid: int | None,
    word: str,
    stop: dict[str, Any],
    failures: list[dict[str, Any]],
    remote: Callable[[], Awaitable[str | None]] | None = None,
) -> dict[str, Any] | None:
    """Stop the process ``record`` names; return ``stop`` with its pid once it is stopped.

    The record is dropped only when its process is stopped, has exited, or is another
    program that took the pid. ``remote`` is tried only when the process can be
    neither confirmed nor ruled out, for example one under another identity. A
    process relay cannot confirm or stop keeps its record and is reported in
    ``failures`` with the reason; it is never dropped silently. ``stop`` holds the
    entry's site, kind and detail.
    """

    def keep(reason: str) -> None:
        where = str(record) if pid is None else f"{record}, pid {pid}"
        failures.append(
            {
                "site": stop["site"],
                "error": RefusalReason.UNREACHABLE.value,
                "detail": f"{stop['detail']} was not stopped ({where}): {reason}",
            }
        )

    if pid is None:
        keep("the record names no process id")
        return None
    state, reason = _probe(pid, word)
    if state == "unknown":
        failed = None if remote is None else await remote()
        if remote is None or failed is not None:
            keep(reason if failed is None else f"{reason}; {failed}")
            return None
        with contextlib.suppress(OSError):
            record.unlink()
        return {**stop, "pid": pid, "detail": f"{stop['detail']}, stopped through its stop route"}
    if state == "relay":
        failed = _terminate(pid)
        if failed is not None:
            keep(failed)
            return None
    with contextlib.suppress(OSError):
        record.unlink()
    return {**stop, "pid": pid} if state == "relay" else None


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


async def _stop_connectors(
    paths: RelayPaths, operations: dict[str, str], site: str | None, failures: list[Any]
) -> list[Any]:
    """Stop ``clio-relay connect`` holders: all of them, or those of ``site``'s operations."""
    from clio_relay.serve.desktop import CONNECTOR_PIDFILE

    stopped: list[dict[str, Any]] = []
    for pidfile in sorted((paths.state_dir / "serve" / "connect").glob(f"*/{CONNECTOR_PIDFILE}")):
        operation = pidfile.parent.name
        owner = operations.get(operation, LOCAL)
        if site is not None and owner != site:
            continue
        pid = _read_pid(pidfile)
        if pid == os.getpid():
            continue
        stop = {
            "site": owner,
            "kind": "connector",
            "detail": f"clio-relay connect for service {operation}",
        }
        found = await _stop_record(pidfile, pid, "connect", stop, failures)
        if found is not None:
            stopped.append(found)
    return stopped


async def _stop_servers(paths: RelayPaths, failures: list[Any]) -> list[dict[str, Any]]:
    """Stop recorded ``serve --http`` processes; this one exits after it answers."""
    stopped: list[dict[str, Any]] = []
    for record in sorted(servers_dir(paths).glob("*.json")):
        stop = {
            "site": LOCAL,
            "kind": "local_server",
            "detail": "a detached clio-relay serve --http",
        }
        try:
            data: Any = json.loads(record.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            failures.append(
                {
                    "site": LOCAL,
                    "error": RefusalReason.UNREACHABLE.value,
                    "detail": f"{stop['detail']} was not stopped ({record}): "
                    f"its record cannot be read: {exc}",
                }
            )
            continue
        fields: dict[str, Any] = cast("dict[str, Any]", data) if isinstance(data, dict) else {}
        pid = fields.get("pid") if isinstance(fields.get("pid"), int) else None
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

        async def remote(fields: dict[str, Any] = fields) -> str | None:
            return await _http_stop(fields)

        found = await _stop_record(record, pid, "serve", stop, failures, remote)
        if found is not None:
            stopped.append(found)
    return stopped


def _exit_self() -> None:
    if os.name == "nt":
        os._exit(0)
    os.kill(os.getpid(), signal.SIGTERM)


def _reap_visitors() -> list[dict[str, Any]]:
    from clio_relay.frp_visitor_reconciliation import reconcile_stale_frp_visitors

    result = reconcile_stale_frp_visitors()
    return [
        {"site": LOCAL, "kind": "frp visitor", "pid": pid, "detail": "an orphaned frp visitor"}
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
    stopped.extend(await _stop_connectors(service.paths, operations, site, failures))
    if site is None:
        try:
            stopped.extend(_reap_visitors())
        except Exception as exc:
            failures.append(
                {"site": LOCAL, "error": RefusalReason.UNREACHABLE.value, "detail": str(exc)}
            )
        stopped.extend(await _stop_servers(service.paths, failures))
    hop_stopped = {item["site"] for item in stopped if item["kind"] == "hop_proxy"}
    return {
        "schema": SCHEMA,
        "stopped": stopped,
        "still_running": _still_running(service, {entry.id for entry in entries}, hop_stopped),
        "failures": failures,
    }
