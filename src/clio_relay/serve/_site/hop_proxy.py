# ruff: noqa
# It runs on the site's own Python, which may be old: no walrus, no newer syntax.
"""A CONNECT proxy on a cluster's login side, for compute nodes that cannot dial out.

Relay runs it with ``python3 -c`` through the adapter's login-side command
(the worker's ``hop_exec`` or ``exec``), inside ``clio_light`` (lowest CPU
and I/O priority, capped). Stdlib only: it runs on the site's own Python.

One hop runs per user and site, in ``hop_dir``. Its daemon holds an ``flock``
on ``hop_dir/run.lock`` for its life, so a second never starts; ``start``
reuses the running one when it lets the same target through. Each job it
serves holds a lease, ``hop_dir/leases/<operation>.json``, and the hop exits
once no lease is left, so it never outlives the jobs it serves. A lease ends
when relay releases it, or on its own when the job's connector wrote
``serve/stopped.json``, when its ``serve/heartbeat`` is older than
HEARTBEAT_STALE_S, when the job left the queue before it started (only for a
job the queue showed by name when relay leased it), or when relay never said
which job it was within SUBMIT_GRACE_S. It also exits once it carried a
connection and then nothing used it for ``idle_exit`` seconds.

Modes:

- ``start hop_dir operation target_host target_port bind_host advertise idle_exit``:
  lease the hop for an operation and print ``{"pid", "host", "port", "token"}``.
  "-" as advertise means this host's own name.
- ``lease hop_dir operation job_dir job_name``: say which job holds the lease.
- ``release hop_dir operation``: end the lease.
- ``shutdown hop_dir -``: stop the running hop and drop every lease; print
  ``{"running", "pid"?}``. Jobs keep running, but their served ports lose the hop.
"""

import base64
import errno
import fcntl
import json
import os
import re
import secrets
import signal
import socket
import subprocess
import sys
import threading
import time
from typing import Any, Dict, List, Optional

HEARTBEAT_STALE_S = 180
SUBMIT_GRACE_S = 900
QUEUE_GRACE_S = 120
QUEUED_MAX_S = 86400
WATCH_S = 30

mode = sys.argv[1]
hop_dir = os.path.expanduser(sys.argv[2])
operation = sys.argv[3]
if not re.match(r"^[A-Za-z0-9_.-]+$", operation):
    sys.exit("hop: bad operation id")
leases = os.path.join(hop_dir, "leases")
info_path = os.path.join(hop_dir, "hop.json")


def write_private(path: str, data: Dict[str, Any]) -> None:
    tmp = path + ".tmp"
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as handle:
        handle.write(json.dumps(data))
    os.replace(tmp, path)


def read_json(path: str) -> Optional[Dict[str, Any]]:
    try:
        with open(path) as handle:
            return json.load(handle)
    except (OSError, ValueError):
        return None


def lease_path(op: str) -> str:
    return os.path.join(leases, op + ".json")


def open_lock(name: str) -> int:
    return os.open(os.path.join(hop_dir, name), os.O_RDWR | os.O_CREAT, 0o600)


def try_lock(fd: int) -> bool:
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return True
    except OSError as exc:
        if exc.errno in (errno.EAGAIN, errno.EACCES, errno.EWOULDBLOCK):
            return False
        raise


def in_queue(job_name: str) -> Optional[bool]:
    """True or False when the queue answers, None when there is none to ask."""
    try:
        result = subprocess.run(
            ["squeue", "-h", "-n", job_name, "-o", "%i"],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            timeout=20,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    if result.returncode != 0:
        return None
    return bool(result.stdout.strip())


def lease_alive(lease: Dict[str, Any], now: float) -> bool:
    job_dir = lease.get("job_dir")
    at = float(lease.get("at", 0))
    if not job_dir:
        return now - at < SUBMIT_GRACE_S
    serve = os.path.join(job_dir, "serve")
    if os.path.exists(os.path.join(serve, "stopped.json")):
        return False
    try:
        return now - os.stat(os.path.join(serve, "heartbeat")).st_mtime < HEARTBEAT_STALE_S
    except OSError:
        pass
    if lease.get("queued") and lease.get("job_name"):
        queued = in_queue(lease["job_name"])
        if queued is not None:
            return queued or now - at < QUEUE_GRACE_S
    return now - at < QUEUED_MAX_S


def live_leases() -> List[Dict[str, Any]]:
    now = time.time()
    alive: List[Dict[str, Any]] = []
    try:
        names = os.listdir(leases)
    except OSError:
        return alive
    for name in names:
        if not name.endswith(".json"):
            continue
        path = os.path.join(leases, name)
        lease = read_json(path)
        if lease is not None and lease_alive(lease, now):
            alive.append(lease)
        else:
            try:
                os.unlink(path)
            except OSError:
                pass
    return alive


if mode == "shutdown":
    if not os.path.isdir(hop_dir):
        print(json.dumps({"running": False}))
        sys.exit(0)
    start_lock = open_lock("start.lock")
    fcntl.flock(start_lock, fcntl.LOCK_EX)
    run_lock = open_lock("run.lock")
    if try_lock(run_lock):
        print(json.dumps({"running": False}))
        sys.exit(0)
    info = read_json(info_path) or {}
    pid = 0
    try:
        pid = int(info.get("pid", 0))
        os.kill(pid, signal.SIGTERM)
    except (OSError, ValueError):
        pass
    deadline = time.time() + 10
    while not try_lock(run_lock):
        if time.time() > deadline:
            sys.exit("hop: the hop on this site did not stop")
        time.sleep(0.2)
    names = os.listdir(leases) if os.path.isdir(leases) else []
    for path in [info_path] + [os.path.join(leases, name) for name in names]:
        try:
            os.unlink(path)
        except OSError:
            pass
    print(json.dumps({"running": True, "pid": pid}))
    sys.exit(0)

if not os.path.isdir(leases):
    os.makedirs(leases, 0o700)

if mode == "release":
    try:
        os.unlink(lease_path(operation))
    except OSError:
        pass
    sys.exit(0)

if mode == "lease":
    job_dir, job_name = sys.argv[4], sys.argv[5]
    lease = read_json(lease_path(operation)) or {"operation": operation, "at": time.time()}
    lease["job_dir"] = job_dir
    lease["job_name"] = job_name
    lease["queued"] = bool(job_name and in_queue(job_name))
    write_private(lease_path(operation), lease)
    sys.exit(0)

if mode != "start":
    sys.exit("hop: unknown mode " + mode)

target_host, target_port = sys.argv[4].lower(), int(sys.argv[5])
bind_host = sys.argv[6]
advertise = "" if sys.argv[7] == "-" else sys.argv[7]
idle_exit = float(sys.argv[8])
target = "%s:%d" % (target_host, target_port)

start_lock = open_lock("start.lock")
fcntl.flock(start_lock, fcntl.LOCK_EX)
run_lock = open_lock("run.lock")
if not try_lock(run_lock):
    info = read_json(info_path) or {}
    if info.get("target") == target:
        write_private(lease_path(operation), {"operation": operation, "at": time.time()})
        print(json.dumps(dict((k, info[k]) for k in ("pid", "host", "port", "token"))))
        sys.exit(0)
    if live_leases():
        sys.exit("hop: the hop on this site serves %s for live jobs" % info.get("target"))
    try:
        os.kill(int(info.get("pid", 0)), signal.SIGTERM)
    except (OSError, ValueError):
        pass
    deadline = time.time() + 10
    while not try_lock(run_lock):
        if time.time() > deadline:
            sys.exit("hop: the old hop on this site did not stop")
        time.sleep(0.2)

# No daemon runs, so no lease is live.
for name in os.listdir(leases):
    try:
        os.unlink(os.path.join(leases, name))
    except OSError:
        pass

token = secrets.token_urlsafe(24)
expected = "basic " + base64.b64encode(("relay:" + token).encode()).decode()
server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
server.bind((bind_host, 0))
server.listen(64)
port = server.getsockname()[1]
host = advertise or socket.getfqdn()
write_private(lease_path(operation), {"operation": operation, "at": time.time()})

child = os.fork()
if child:
    info = {"pid": child, "host": host, "port": port, "token": token}
    write_private(info_path, dict(info, target=target))
    print(json.dumps(info), flush=True)
    os._exit(0)
# The daemon keeps run.lock; start.lock goes with the parent.
os.close(start_lock)
os.setsid()
null = os.open(os.devnull, os.O_RDWR)
for fd in (0, 1, 2):
    os.dup2(null, fd)
threading.stack_size(256 * 1024)

lock = threading.Lock()
active = 0
served = False
last_used = time.monotonic()


def pump(source: socket.socket, sink: socket.socket) -> None:
    try:
        while True:
            data = source.recv(65536)
            if not data:
                break
            sink.sendall(data)
    except OSError:
        pass
    finally:
        for sock in (source, sink):
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass


def handle(client: socket.socket) -> None:
    global active, served, last_used
    client.settimeout(30)
    request = b""
    try:
        while b"\r\n\r\n" not in request and len(request) < 16384:
            chunk = client.recv(4096)
            if not chunk:
                return
            request += chunk
        head, _, rest = request.partition(b"\r\n\r\n")
        lines = head.decode("latin-1").split("\r\n")
        parts = lines[0].split()
        headers: Dict[str, str] = {}
        for line in lines[1:]:
            name, _, value = line.partition(":")
            headers[name.strip().lower()] = value.strip()
        if headers.get("proxy-authorization", "").lower() != expected.lower():
            client.sendall(b"HTTP/1.1 407 Proxy Authentication Required\r\n\r\n")
            return
        if len(parts) < 2 or parts[0].upper() != "CONNECT":
            client.sendall(b"HTTP/1.1 405 Method Not Allowed\r\n\r\n")
            return
        want_host, _, want_port = parts[1].rpartition(":")
        if want_host.strip("[]").lower() != target_host or want_port != str(target_port):
            client.sendall(b"HTTP/1.1 403 Forbidden\r\n\r\n")
            return
        upstream = socket.create_connection((target_host, target_port), timeout=30)
        upstream.settimeout(None)
        client.settimeout(None)
        client.sendall(b"HTTP/1.1 200 Connection established\r\n\r\n")
        if rest:
            upstream.sendall(rest)
    except OSError:
        client.close()
        return
    with lock:
        active += 1
        served = True
    try:
        back = threading.Thread(target=pump, args=(upstream, client), daemon=True)
        back.start()
        pump(client, upstream)
        back.join()
    finally:
        upstream.close()
        client.close()
        with lock:
            active -= 1
            last_used = time.monotonic()


def finish(force: bool) -> None:
    # Hold start.lock to the end, so that no start leases a hop that is going away.
    fd = open_lock("start.lock")
    fcntl.flock(fd, fcntl.LOCK_EX)
    if not force and live_leases():
        os.close(fd)
        return
    try:
        os.unlink(info_path)
    except OSError:
        pass
    os._exit(0)


def started(lease: Dict[str, Any]) -> bool:
    job_dir = lease.get("job_dir")
    return bool(job_dir) and os.path.exists(os.path.join(str(job_dir), "serve", "heartbeat"))


def watch() -> None:
    while True:
        time.sleep(WATCH_S)
        with lock:
            idle = served and active == 0 and time.monotonic() - last_used > idle_exit
        alive = live_leases()
        # A job still in the queue keeps the hop, idle or not.
        idle = idle and all(started(lease) for lease in alive)
        if idle or not alive:
            finish(idle)


def on_term(signum: int, frame: Any) -> None:
    os._exit(0)


signal.signal(signal.SIGTERM, on_term)
threading.Thread(target=watch, daemon=True).start()
while True:
    connection, _ = server.accept()
    threading.Thread(target=handle, args=(connection,), daemon=True).start()
