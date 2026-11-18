# ruff: noqa
# It runs on the site's own Python, which may be old: no walrus, no newer syntax.
"""A CONNECT proxy on a cluster's login side, for compute nodes that cannot dial out.

Relay runs it with ``python3 -c`` through the adapter's login-side command
(the worker's ``hop_exec`` or ``exec``). It lets one target through, needs the
token it prints, daemonizes, and prints ``{"pid", "host", "port", "token"}``.
After it has carried a connection it exits once nothing has used it for
``idle_exit`` seconds, so a hop whose service ended does not linger. Stdlib
only: it runs on the site's own Python.

Arguments: target_host target_port bind_host advertise_host idle_exit label.
"-" as advertise_host means this host's own name.
"""

import base64
import json
import os
import secrets
import socket
import sys
import threading
import time

target_host, target_port = sys.argv[1].lower(), int(sys.argv[2])
bind_host = sys.argv[3]
advertise = "" if sys.argv[4] == "-" else sys.argv[4]
idle_exit = float(sys.argv[5])
token = secrets.token_urlsafe(24)
expected = "basic " + base64.b64encode(f"relay:{token}".encode()).decode()

server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
server.bind((bind_host, 0))
server.listen(64)
port = server.getsockname()[1]
host = advertise or socket.getfqdn()

child = os.fork()
if child:
    print(json.dumps({"pid": child, "host": host, "port": port, "token": token}), flush=True)
    os._exit(0)
os.setsid()
null = os.open(os.devnull, os.O_RDWR)
for fd in (0, 1, 2):
    os.dup2(null, fd)

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
        headers: dict[str, str] = {}
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


def watch() -> None:
    while True:
        time.sleep(30)
        with lock:
            idle = served and active == 0 and time.monotonic() - last_used > idle_exit
        if idle:
            os._exit(0)


threading.Thread(target=watch, daemon=True).start()
while True:
    connection, _ = server.accept()
    threading.Thread(target=handle, args=(connection,), daemon=True).start()
