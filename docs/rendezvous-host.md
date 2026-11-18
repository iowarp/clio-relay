# Running a rendezvous host

A service that relay serves on a cluster, such as a ParaView `pvserver`, a
dashboard or an MCP server, has to reach your desktop. Compute nodes rarely
accept incoming connections, so both ends dial out to a meeting point instead:
the rendezvous host. It runs the frp server (`frps`). Relay starts an frp
client beside the service and another on your desktop, and frp joins them.

The rendezvous host only carries bytes. It stores no jobs, no records and no
secrets of the services it carries. Every service port relay gives you is
bound to `127.0.0.1` on your desktop. Nothing is exposed publicly.

You don't need a rendezvous host for a site you reach over `ssh`. Without one,
relay holds a single `ssh -L` forward for each service. A rendezvous host is
better when the desktop sleeps or changes networks, because frp reconnects on
its own. On Windows, the `ssh -L` forward also doesn't survive relay
restarting, so `describe` marks that endpoint as not durable.

## What you need

- A machine with a public address that both your desktop and the cluster can
  reach: a small VM is enough.
- frp 0.69 or later (`frps` there, `frpc` on your desktop and on the cluster).
- A long random token, shared by the server and every client.

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
    "token_file": "~/.config/frp/token",
    "frpc": "frpc"
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

Set `"direct": true` to try a direct peer-to-peer path (XTCP) first. When it
can't be made, frp falls back to the path through the rendezvous host.

## Per-site settings

Each entry under `sites` says how that site's services reach the rendezvous
host:

| Setting | Meaning |
| --- | --- |
| `frpc` | Where `frpc` is on the site. Without it, set `frpc_binary` to a local `frpc` built for the site and relay copies it in with the job. |
| `address`, `port` | The rendezvous host as the site sees it, when that differs from the desktop's view. |
| `hop` | `none`: compute nodes dial out themselves. `login`: relay starts a small proxy on the login node for the life of the service, and the node's frp client goes through it. `http_proxy`: use the site's own HTTP proxy, named in `proxy_url`. |
| `hop_advertise` | The login node's address as compute nodes see it, when its host name doesn't resolve there. |
| `path` | `auto` (the default) uses frp when a rendezvous host is set, and `ssh -L` otherwise. `frp` or `ssh` forces one. |
| `stop_grace_s` | Seconds a service gets to write its final files after a stop. 30 by default. |

The login hop asks for a per-service password and only connects to the
rendezvous host, so other users on the login node can't use it to reach
anything else.

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

- `frpc` on your desktop logs `login to server success` when the address and
  token are right.
- A wrong CA for `wss` shows a certificate error in relay's forward log, under
  `serve/forwards/<operation>/forward.log` in the state directory.
- On the cluster, each service's job directory has `serve/frpc.log` and
  `serve/launch.log`, which say when the service's ports came up and when frp
  registered them.
