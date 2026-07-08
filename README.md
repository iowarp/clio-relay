<p align="center">
  <img src="docs/assets/clio-relay-banner.png" alt="clio-relay banner">
</p>

<h1 align="center">clio-relay</h1>

`clio-relay` lets a desktop tool submit work to a remote cluster, follow it while it runs, and collect logs, artifacts, progress, and provenance without putting job state in the network tunnel.

It is a piece of the federation layer for [`clio-agent`](https://github.com/iowarp/clio-agent): a local CLIO experience can delegate work to a remote machine, keep observing it, detach, reconnect, and clean up after itself. The project is also designed for use outside CLIO. Any client that can call the CLI, HTTP API, or MCP tools can use the same relay model.

## How It Works

`clio-relay` has three long-running roles:

- `desktop`: submits work and exposes CLI, HTTP, and MCP surfaces for local tools.
- `worker`: runs on a configured cluster, leases work, invokes JARVIS-CD, and records results.
- `relay-host`: carries bytes for frp deployments. It does not store jobs or queue state.

The durable boundary is `clio-core`. The filesystem queue in this repository is the development backend for that record contract. Jobs, task timelines, progress, scheduler state, gateway sessions, logs, artifacts, and provenance are recorded there so clients can detach, reconnect, and replay state. JARVIS-CD owns scheduler execution, package behavior, output collection, and provenance.

Transport is replaceable because it only carries HTTP bytes between endpoints:

- Relay mode uses frp through a public relay host. It supports WebSocket/TLS for Cloudflare-style HTTPS infrastructure and raw TCP for environments that provide a direct public port.
- NAT bypass uses frp XTCP to try a direct peer path between desktop and cluster. It is an optimization for lower-latency or higher-volume traffic, with fallback to relay mode and the durable queue.
- SSH forwarding uses local port forwarding through an existing SSH or VPN path. It is useful for closed environments that do not want a public relay.

Remote agent tasks, remote MCP calls, JARVIS pipelines, and gateway sessions all use the same queue and observation model. The transport can change without changing where state lives.

## Install

For normal use, run the released package with `uvx`. No checkout is required.

```powershell
uvx --python 3.12 --from clio-relay clio-relay init
uvx --python 3.12 --from clio-relay clio-relay install-frp
```

Add a cluster. The cluster name and agent executable are local configuration.

```powershell
uvx --python 3.12 --from clio-relay clio-relay cluster add --name my-cluster --ssh-host my-cluster-login --agent-adapter exec --agent-bin agent
uvx --python 3.12 --from clio-relay clio-relay cluster bootstrap --cluster my-cluster
uvx --python 3.12 --from clio-relay clio-relay cluster install-endpoint-service --cluster my-cluster --start --enable
```

## Submit Work

Submit a JARVIS pipeline:

```powershell
uvx --python 3.12 --from clio-relay clio-relay job submit --cluster my-cluster --jarvis-yaml .\pipeline.yaml
uvx --python 3.12 --from clio-relay clio-relay job watch <job-id> --cluster my-cluster
uvx --python 3.12 --from clio-relay clio-relay job read-log <job-id> --cluster my-cluster --stream stdout
uvx --python 3.12 --from clio-relay clio-relay job list-artifacts <job-id> --cluster my-cluster
```

Expose relay tools to an agent:

```powershell
uvx --python 3.12 --from clio-relay clio-relay agent render-mcp-config --output .\clio-relay-agent.config.toml
uvx --python 3.12 --from clio-relay clio-relay agent run --cluster my-cluster --prompt /path/on/cluster/prompt.md --mcp-config /path/on/cluster/clio-relay-agent.config.toml
```

## Observe Remote Agent Work

Remote agents can emit structured task timeline events while they work. This is useful when a UI needs to show discovery and planning before the final answer exists.

```powershell
uvx --python 3.12 --from clio-relay clio-relay job tasks <job-id> --cluster my-cluster
uvx --python 3.12 --from clio-relay clio-relay job record-task-event <task-id> --cluster my-cluster --event-type dataset_found --label dataset --summary "found staged dataset" --path-ref /mnt/common/datasets/red_sea_001
uvx --python 3.12 --from clio-relay clio-relay job task-events <task-id> --cluster my-cluster --cursor 1
```

The same contract is available over HTTP at `/tasks/{task_id}/events`, `/tasks/{task_id}/events/sse`, and `/tasks/{task_id}/events/ws`, and through MCP tools `relay_record_task_event` and `relay_watch_task_events`.

## Manage Gateway Sessions

Long-running visualization services should be tracked as durable gateway sessions. A session records the scheduler job, node, logs, published or forwarded endpoint, health metadata, and reconnect hints.

```powershell
uvx --python 3.12 --from clio-relay clio-relay gateway create --cluster my-cluster --name paraview-red-sea --gateway-json-file .\gateway.json
uvx --python 3.12 --from clio-relay clio-relay gateway update <session-id> --cluster my-cluster --state ready --scheduler-job-id 12345 --node compute-01 --gateway-json-file .\gateway-ready.json
uvx --python 3.12 --from clio-relay clio-relay gateway get <session-id> --cluster my-cluster
uvx --python 3.12 --from clio-relay clio-relay gateway close <session-id> --cluster my-cluster
```

The HTTP API exposes `/gateway-sessions`, `/gateway-sessions/{session_id}`, and `/gateway-sessions/{session_id}/close`. MCP tools expose the same create, read, update, and close operations.

## Choose Transport

For a public relay through Cloudflare or another HTTPS edge, use frp with `transport.protocol = "wss"`:

```powershell
$env:CLIO_RELAY_FRP_TOKEN = "<shared-frp-token>"
$env:CLIO_RELAY_STCP_SECRET = "<shared-stcp-secret>"
uvx --python 3.12 --from clio-relay clio-relay cluster add --name my-cluster --ssh-host my-cluster-login --frp-server-addr relay.example.edu
uvx --python 3.12 --from clio-relay clio-relay relay-host render-frpc-config --cluster my-cluster --local-port 8848
uvx --python 3.12 --from clio-relay clio-relay relay-host render-frpc-visitor-config --cluster my-cluster --bind-port 8765
uvx --python 3.12 --from clio-relay clio-relay relay-host test-http-transport --cluster my-cluster --local-bind-port 18765
```

For closed environments where SSH or VPN already exists, use SSH local forwarding:

```powershell
uvx --python 3.12 --from clio-relay clio-relay relay-host test-ssh-transport --cluster my-cluster --local-bind-port 18766 --remote-api-port 8766 --session-id relay-ssh-test
```

To leave the remote API alive for a desktop detach and later reattach:

```powershell
uvx --python 3.12 --from clio-relay clio-relay session start --cluster my-cluster --session-id desktop-session --remote-api-port 8766 --replace
uvx --python 3.12 --from clio-relay clio-relay session status --cluster my-cluster --session-id desktop-session
uvx --python 3.12 --from clio-relay clio-relay session teardown --cluster my-cluster --session-id desktop-session
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
uv sync
uv run ruff check --fix
uv run ruff format
uv run pyright
uv run pytest
```

The GitHub workflow runs lint, type checks, tests, package builds, and artifact validation. Publishing to PyPI is configured through trusted publishing for the `iowarp/clio-relay` repository.
