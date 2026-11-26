# Running a rendezvous host

A service that relay serves on a cluster, such as a ParaView `pvserver`, a
dashboard or an MCP server, has to reach your desktop. Compute nodes rarely
accept incoming connections, so both ends dial out to a meeting point instead:
the rendezvous host. It runs the frp server (`frps`). Relay starts an frp
client beside the service and its own frp visitor on your desktop, and frp
joins them.

The rendezvous host only carries bytes. It stores no jobs, no records and no
secrets of the services it carries. Every service port relay gives you is
bound to `127.0.0.1` on your desktop. Nothing is exposed publicly.

You don't need a rendezvous host for a site you reach over `ssh`. Without one,
relay holds a single `ssh -L` forward for each service. A rendezvous host is
better when the desktop sleeps or changes networks, because frp reconnects on
its own. The `ssh -L` forward outlives relay restarting: a new relay adopts it,
or starts it again at once when it has died. A site that signs in
interactively is the exception, since its forward rides the sign-in
connection, which ends with relay, so `describe` marks that endpoint as not
durable.

## What you need

- A machine with a public address that both your desktop and the cluster can
  reach: a small VM is enough.
- frp 0.69 or later: `frps` there and `frpc` on the cluster.
- A long random token, shared by the server and every client.

Your desktop needs no frp program. Relay runs the desktop end itself, in
Python, so nothing extra is installed and no antivirus has to allow an
`frpc` binary. It speaks the `tcp`, `websocket` and `wss` transports, with or
without frp's stream multiplexing. It doesn't speak `kcp` or `quic`, so with
those `path: auto` uses `ssh -L`.

## Run the server

A minimal `frps.toml`:

```toml
bindPort = 7000
auth.method = "token"
auth.token = "{{ .Envs.FRP_TOKEN }}"
```

Start it with the token in the environment:

```sh
FRP_TOKEN="$(cat ~/.config/frp/token)" frps -c frps.toml
```

If the cluster's network only lets HTTPS out, put frps behind a TLS endpoint
and use `wss`. Relay always verifies the server's certificate. It uses the CA
bundle you name, or the system roots when you name none.

## Point relay at it

Relay reads the setting from `serve/rendezvous.json` in its state directory
(`~/.local/state/clio-relay` on Linux and macOS,
`%LOCALAPPDATA%\clio-relay\state` on Windows, or `CLIO_RELAY_STATE_DIR`), or
from the file named by `CLIO_RELAY_RENDEZVOUS_FILE`:

```json
{
  "rendezvous": {
    "address": "rendezvous.example.org",
    "port": 7000,
    "protocol": "tcp",
    "token_file": "~/.config/frp/token"
  },
  "sites": {
    "my-cluster": {
      "frpc": "~/bin/frpc",
      "hop": "login"
    }
  }
}
```

The file names where the token is, never the token itself. Use `token_env`
instead of `token_file` to read it from an environment variable.

For `wss`, set `"protocol": "wss"`, the port of the TLS endpoint, and
`"ca_file"` when the certificate isn't signed by a public CA. Set
`"server_name"` when the certificate carries a name other than `address`.

An older file may still name a desktop `"frpc"`. Relay ignores it.

Set `"direct": true` to try a direct peer-to-peer path (XTCP) first. When it
can't be made, frp falls back to the path through the rendezvous host. Relay
never switches silently: the service's status and `clio-relay connect` say
which path is in use, and why the direct one failed. Relay's desktop visitor
punches the hole and carries the direct path over QUIC itself, as frpc
does, so no frp program runs on the desktop.

When the desktop's network leaves only through a proxy, as it does for a
server CLIO runs inside its fence, the visitor reaches the rendezvous host
through it, as frpc does: `transport.proxyURL` if set, else `HTTPS_PROXY`
for `wss` (`HTTP_PROXY` otherwise), then `ALL_PROXY`, skipping hosts named in
`NO_PROXY`. It speaks HTTP `CONNECT` and SOCKS5. The direct path is UDP and
can't go through a proxy, so behind one it fails and says why.

Two settings shape the direct path:

- `stun_server` is the STUN server both ends ask for their public address.
  The default is `stun.nextcloud.com:3478`. frp needs a server that supports
  RFC 5780 (it answers from a second address), so general-purpose servers such
  as Cloudflare's or Google's don't work. Use your own if you run one.
- `direct_wait_s` is how long a connection waits for the direct path before it
  goes through the rendezvous host. The default is 10 seconds, and the most is
  19, because frp gives up on its own at 20. The punch starts as soon as the
  forward starts, so the path is usually open before the first connection. If
  the direct path never works from a place, turn `direct` off there so
  connections don't wait.

The desktop punches from the address that leads to the STUN server, so the
public address STUN reports is the one the punch leaves from. A split-tunnel
VPN that carries the site's network doesn't take the punch into the tunnel.

## Per-site settings

Each entry under `sites` says how that site's services reach the rendezvous
host:

| Setting | Meaning |
| --- | --- |
| `frpc` | Where `frpc` is on the site. Without it, set `frpc_binary` to a local `frpc` built for the site and relay copies it in with the job. |
| `address`, `port` | The rendezvous host as the site sees it, when that differs from the desktop's view. |
| `hop` | `none`: compute nodes dial out themselves. `login`: relay starts a small proxy on the login node while the site's services run, and the node's frp client goes through it. `http_proxy`: use the site's own HTTP proxy, named in `proxy_url`. |
| `hop_advertise` | The login node's address as compute nodes see it, when its host name doesn't resolve there. |
| `hop_dir` | Where the login hop keeps its lock, leases and password on the site, one directory per site. By default `hop` under the site's own relay directory (its `remote_dir`), so site clean removes it with that directory. |
| `login_limits` | What the login hop may use: `cpu_quota_percent`, `memory_mb`, `address_space_mb` and `cpu_s`. |
| `path` | `auto` (the default) uses frp when a rendezvous host is set and frp can work: the site has an `frpc` or `frpc_binary`, relay's visitor speaks the transport, and the token reads. Otherwise it uses `ssh -L`. `frp` or `ssh` forces one. |
| `stun_server` | The STUN server for this site's frp client, when the site can't reach the rendezvous one. |
| `stop_grace_s` | Seconds a service gets to write its final files after a stop. 30 by default. |

The login hop asks for a password that only relay and your jobs have, and only
connects to the rendezvous host, so other users on the login node can't use it
to reach anything else.

### The login hop on a login node

The hop is light and never outlives the jobs it serves:

- It runs at the lowest CPU and I/O priority, capped the way the
  [worker](adapters/worker.md#on-the-cluster) is.
- One hop runs per user and site. Services that start while it runs reuse it.
- Each service job holds a lease on it. The lease ends when relay releases it
  or when the job is over: its connector wrote `serve/stopped.json`, its
  `serve/heartbeat` (touched every 30 s) is three minutes old, or it left the
  Slurm queue before it started. A lease relay never tied to a job ends after
  15 minutes.
- The hop exits once no lease is left. It also exits an hour after its last
  connection ends, unless a job it serves is still waiting in the queue.

## Using a remote relay

When relay itself runs away from your desktop, for example on a lab server you
reach over HTTP, the ports it reports are on that server. The endpoint then
carries a command:

```sh
clio-relay connect <handle>
```

Run it on your desktop. It binds the same ports on `127.0.0.1` through the
rendezvous host and binds them again after a drop. The handle doesn't carry
the frp token: your desktop's own `rendezvous.json` supplies it.

## Checking it works

- Relay's visitor writes its log to `serve/forwards/<operation>/forward.log`
  in the state directory, in frpc's format. It logs `login to server success`
  when the address and token are right, and frps's own reason when they
  aren't.
- A wrong CA for `wss` shows a certificate error in that log.
- On the cluster, each service's job directory has `serve/frpc.log` and
  `serve/launch.log`, which say when the service's ports came up and when frp
  registered them.
