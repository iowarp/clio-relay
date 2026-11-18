# Fixture sites

Small container sites that stand in for real clusters while relay is tested. They run
on any Docker host; the rework runs them on Ares.

| Site | Container | What it is |
| --- | --- | --- |
| `ssh` | `fx-ssh` | A plain SSH host. User `relay`, key sign-in only, with scp and sftp |
| `slurm` | `fx-slurm` | The SSH host plus a one-node Slurm cluster with accounting (`sacct`) |
| `pbs` | `fx-pbs` | The SSH host plus a one-node OpenPBS server, built from source |
| `otp` | `fx-otp` | An SSH host that asks for a one-time code (TOTP) after the key |
| `frps` | `fx-frps` | An frp 0.69.1 rendezvous: tcp and websocket, plus wss through a TLS front |
| `secret` | `fx-secret` | An HTTP and WebSocket service that checks a secret |
| `counter` | `fx-counter` | A small MCP server that keeps a counter and runs one tool as a task |

## Bring them up

```bash
scripts/ares/fixtures.sh up       # builds the images the first time
scripts/ares/fixtures.sh status   # containers and the port map
uv run pytest tests/test_fixtures_smoke.py
scripts/ares/fixtures.sh down     # add --images to remove the images as well
```

`up` writes keys, secrets and an env file under `~/clio-relay-rework/state/w2-fixtures`
(set `FX_STATE_DIR` to move it). The env file names every port and secret file, and
`ssh -F <state>/ssh_config fx-slurm` reaches a site. Ports bind to `127.0.0.1` from 47100
up, skipping any port already in use. Set `FX_SITES` to start a subset.

## What to know

- Nothing secret is in an image. Each container gets only the keys and secrets its role
  needs, copied in at start.
- Every container, network and image is labelled `clio-relay-rework=1` and
  `clio-relay-rework.owner=w2-fixtures`. `down` removes only what carries both labels.
- The scheduler sites and the plain SSH site also carry the secret service and the
  counter server as `clio-fixture-secret-service` and `clio-fixture-counter-mcp`, so a
  test can start them on a site, over SSH.
- frps does not serve TLS itself. The wss port is a TLS front (socat) that hands the
  connection to frps, the way a proxy such as Cloudflare does in production.
