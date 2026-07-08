<p align="center">
  <img src="docs/assets/clio-relay-banner.png" alt="clio-relay banner">
</p>

<h1 align="center">clio-relay</h1>

<p align="center">
  <img src="docs/assets/clio-relay-logo.png" alt="clio-relay logo" width="96">
</p>

`clio-relay` lets a desktop tool submit work to a remote cluster, follow it while it runs, and collect logs, artifacts, progress, and provenance without putting job state in the network tunnel.

It is a piece of the federation layer for [`clio-agent`](https://github.com/iowarp/clio-agent): a local CLIO experience can delegate work to a remote machine, keep observing it, detach, reconnect, and clean up after itself. The project is also designed for use outside CLIO. Any client that can call the CLI, HTTP API, or MCP tools can use the same relay model.

## What It Does

- Submits JARVIS-CD pipelines to configured clusters.
- Runs remote agent tasks and remote MCP calls through the same queue.
- Streams stdout, stderr, events, progress, artifacts, and provenance back to the desktop side.
- Records structured task timelines for remote agent workflows, with cursor replay for reconnecting UIs.
- Tracks durable scheduler-backed gateway sessions for services such as cluster-side visualization servers.
- Supports reconnect and replay through durable queue records.
- Keeps network transport separate from application state.
- Supports frp over WebSocket/TLS, frp over TCP, SSH local port forwarding, and optional frp XTCP probing.
- Lets a desktop app detach from a remote session or tear down the remote relay processes explicitly.

## How It Works

`clio-relay` has three roles:

- `desktop`: submits work and exposes CLI, HTTP, and MCP surfaces for local tools.
- `worker`: runs on a configured cluster, leases work, invokes JARVIS-CD, and records results.
- `relay-host`: carries bytes for frp deployments. It does not store jobs or queue state.

The durable boundary is `clio-core`. The filesystem queue in this repository is the development backend for that record contract. JARVIS-CD owns scheduler execution, package behavior, output collection, and provenance. frp and SSH forwarding only carry HTTP bytes between endpoints.

## Install

```powershell
uv sync
uv run clio-relay init
uv run clio-relay install-frp
```

Add a cluster. The cluster name and agent executable are local configuration.

```powershell
uv run clio-relay cluster add --name my-cluster --ssh-host my-cluster-login --agent-adapter exec --agent-bin agent
uv run clio-relay cluster bootstrap --cluster my-cluster
uv run clio-relay cluster install-endpoint-service --cluster my-cluster --start --enable
```

## Submit Work

Submit a JARVIS pipeline:

```powershell
uv run clio-relay job submit --cluster my-cluster --jarvis-yaml .\pipeline.yaml
uv run clio-relay job watch <job-id> --cluster my-cluster
uv run clio-relay job read-log <job-id> --cluster my-cluster --stream stdout
uv run clio-relay job list-artifacts <job-id> --cluster my-cluster
```

Expose relay tools to an agent:

```powershell
uv run clio-relay agent render-mcp-config --output .\clio-relay-agent.config.toml
uv run clio-relay agent run --cluster my-cluster --prompt /path/on/cluster/prompt.md --mcp-config /path/on/cluster/clio-relay-agent.config.toml
```

Run live acceptance against the builtin JARVIS LAMMPS package:

```powershell
uv run clio-relay live-test --cluster ares --jarvis-yaml .\examples\ares-lammps\pipeline.yaml --monitor-pattern "Loop time"
```

## Observe Remote Agent Work

Remote agents can emit structured task timeline events while they work. This is useful when a UI needs to show discovery and planning before the final answer exists.

```powershell
uv run clio-relay job tasks <job-id> --cluster my-cluster
uv run clio-relay job record-task-event <task-id> --cluster my-cluster --event-type dataset_found --label dataset --summary "found staged dataset" --path-ref /mnt/common/datasets/red_sea_001
uv run clio-relay job task-events <task-id> --cluster my-cluster --cursor 1
```

The same contract is available over HTTP at `/tasks/{task_id}/events`, `/tasks/{task_id}/events/sse`, and `/tasks/{task_id}/events/ws`, and through MCP tools `relay_record_task_event` and `relay_watch_task_events`.

## Manage Gateway Sessions

Long-running visualization services should be tracked as durable gateway sessions. A session records the scheduler job, node, logs, published or forwarded endpoint, health metadata, and reconnect hints.

```powershell
uv run clio-relay gateway create --cluster my-cluster --name paraview-red-sea --gateway-json-file .\gateway.json
uv run clio-relay gateway update <session-id> --cluster my-cluster --state ready --scheduler-job-id 12345 --node compute-01 --gateway-json-file .\gateway-ready.json
uv run clio-relay gateway get <session-id> --cluster my-cluster
uv run clio-relay gateway close <session-id> --cluster my-cluster
```

The HTTP API exposes `/gateway-sessions`, `/gateway-sessions/{session_id}`, and `/gateway-sessions/{session_id}/close`. MCP tools expose the same create, read, update, and close operations.

## Choose Transport

For a public relay through Cloudflare or another HTTPS edge, use frp with `transport.protocol = "wss"`:

```powershell
$env:CLIO_RELAY_FRP_TOKEN = "<shared-frp-token>"
$env:CLIO_RELAY_STCP_SECRET = "<shared-stcp-secret>"
uv run clio-relay cluster add --name my-cluster --ssh-host my-cluster-login --frp-server-addr relay.example.edu
uv run clio-relay relay-host render-frpc-config --cluster my-cluster --local-port 8848
uv run clio-relay relay-host render-frpc-visitor-config --cluster my-cluster --bind-port 8765
uv run clio-relay relay-host test-http-transport --cluster my-cluster --local-bind-port 18765
```

For closed environments where SSH or VPN already exists, use SSH local forwarding:

```powershell
uv run clio-relay relay-host test-ssh-transport --cluster my-cluster --local-bind-port 18766 --remote-api-port 8766 --session-id relay-ssh-test
```

To leave the remote API alive for a desktop detach and later reattach:

```powershell
uv run clio-relay session start --cluster my-cluster --session-id desktop-session --remote-api-port 8766 --replace
uv run clio-relay session status --cluster my-cluster --session-id desktop-session
uv run clio-relay session teardown --cluster my-cluster --session-id desktop-session
```

Use `session teardown --stop-worker` only when the user chooses to clean up the persistent remote worker too.

## Documentation

- [architecture](docs/architecture.md)
- [operations](docs/operations.md)
- [release](docs/release.md)
- [brand prompt](docs/brand.md)
- [ai context](docs/ai/README.md)

## Development

```powershell
uv run ruff check --fix
uv run ruff format
uv run pyright
uv run pytest
```

The GitHub workflow runs lint, type checks, tests, package builds, and artifact validation. Publishing to PyPI is configured through trusted publishing and can be enabled when the repository moves under the `iowarp` organization.
