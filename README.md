<p align="center">
  <img src="docs/assets/clio-relay-banner.png" alt="clio-relay banner">
</p>

<h1 align="center">clio-relay</h1>

`clio-relay` lets a desktop tool submit work to a remote cluster, follow it while it runs, and collect logs, artifacts, progress, and provenance without putting job state in the network tunnel.

It is a piece of the federation layer for [`clio-agent`](https://github.com/iowarp/clio-agent): a local CLIO experience can delegate work to a remote machine, keep observing it, detach, reconnect, and clean up after itself. The project is also designed for use outside CLIO. Any client that can call the CLI, HTTP API, or MCP tools can use the same relay model.

> The current development candidate is `1.0.5`. The immutable `v1.0.0` candidate
> was abandoned before publication after its acceptance runbook rejected the
> staged GNU checksum format; `v1.0.1` was also abandoned before publication
> after protected-main validation exposed a Windows lease-deletion race;
> `v1.0.2` was abandoned before publication when candidate acceptance found a
> strict-mode Spack JSON-array parsing defect in the reviewed runbook;
> `v1.0.3` was abandoned before any reports were produced when candidate
> acceptance found that Windows checkout CRLF bytes had been copied directly
> into a remote shell fixture; `v1.0.4` was abandoned before any reports were
> produced when the corrected fixture reached CMake and exposed that the direct
> Gray-Scott helper did not bind the selected Spack ADIOS2 package's own CMake
> configuration. The latest released live evidence is `0.9.22`; 1.0.5 is not
> release-complete until its immutable-candidate
> reports pass, its exact candidate is published, and the released-artifact
> runs pass again. The policy currently selects the `ares` and `homelab`
> evidence labels; those labels are release configuration, not hardcoded
> product targets, and operators can select additional or different clusters.
> The coordinated JARVIS-CD 1.2.2 and clio-kit 2.3.2 artifacts are exact pins;
> older component artifacts cannot satisfy the 1.0 gate.

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

For normal use, install the released package once as a persistent uv tool. No
checkout is required, and every later command reuses the same isolated tool
environment and cache.

```powershell
uv tool install --python 3.12 --no-config clio-relay
clio-relay init
clio-relay install-frp
```

Add a cluster. The cluster name and agent executable are local configuration.

```powershell
clio-relay cluster add --name my-cluster --ssh-host my-cluster-login --agent-adapter exec --agent-bin agent
clio-relay cluster bootstrap --cluster my-cluster
clio-relay cluster install-endpoint-service --cluster my-cluster --concurrency 4 --kind-concurrency remote_agent=2 --kind-concurrency mcp_call=1 --start --enable
```

## Submit Work

Submit a JARVIS pipeline:

```powershell
clio-relay job submit --cluster my-cluster --jarvis-yaml .\pipeline.yaml
clio-relay job watch <job-id> --cluster my-cluster
clio-relay job read-log <job-id> --cluster my-cluster --stream stdout
clio-relay job list-artifacts <job-id> --cluster my-cluster
```

Expose relay tools to an agent:

```powershell
clio-relay agent render-mcp-config --output .\clio-relay-agent.config.toml
clio-relay agent run --cluster my-cluster --prompt /path/on/cluster/prompt.md --mcp-config /path/on/cluster/clio-relay-agent.config.toml
```

Operators can expose selected tools from any cluster-side stdio MCP server
through the same local relay MCP. Registration is allowlisted, and schema
discovery is an explicit durable job:

```powershell
uv tool install --python 3.12 --no-config C:\artifacts\science_mcp_kit-1.4.0-py3-none-any.whl
clio-relay remote-mcp register --cluster my-cluster --name science --command science-mcp --allow-tool inspect_dataset --profile user
clio-relay remote-mcp refresh --cluster my-cluster --name science
```

User-profile federation requires an exact immutable wheel path (or a unique,
non-editable console installation whose complete `RECORD` closure verifies).
A mutable install or a package-index spec such as `science-mcp-kit==1.4.0`
fails closed and is not exposed to agents.

See [remote MCP federation](docs/remote-mcp-federation.md) for cache,
freshness, alias, collision, profile, and live-acceptance semantics.

## Observe Remote Agent Work

Remote agents can emit structured task timeline events while they work. This is useful when a UI needs to show discovery and planning before the final answer exists.

```powershell
clio-relay job tasks <job-id> --cluster my-cluster
clio-relay job record-task-event <task-id> --cluster my-cluster --event-type dataset_found --label dataset --summary "found staged dataset" --path-ref /mnt/common/datasets/example_001
clio-relay job task-events <task-id> --cluster my-cluster --cursor 1
```

The same contract is available over HTTP at `/tasks/{task_id}/events`, `/tasks/{task_id}/events/sse`, and `/tasks/{task_id}/events/ws`, and through MCP tools `relay_record_task_event` and `relay_watch_task_events`.

## Manage Gateway Sessions

Long-running remote services should be tracked as durable gateway sessions. A session records the scheduler job, node, logs, published or forwarded endpoint, health metadata, and reconnect hints. Production service runtimes should be launched by a JARVIS package or pipeline, which owns application-specific monitoring and emits structured status/events back to the relay.

```powershell
clio-relay gateway create --cluster my-cluster --name live-service-example --gateway-json-file .\gateway.json
clio-relay gateway update <session-id> --cluster my-cluster --state ready --node compute-01 --gateway-json-file .\gateway-ready.json
clio-relay gateway get <session-id> --cluster my-cluster
clio-relay gateway close <session-id> --cluster my-cluster
```

The HTTP API exposes `/gateway-sessions`, `/gateway-sessions/{session_id}`, and `/gateway-sessions/{session_id}/close`. MCP tools expose the same create, read, update, and close operations.

These generic gateway operations manage ordinary endpoint metadata only. They cannot write scheduler identity, relay runtime specifications or ownership intents, connector ownership, or relay owner metadata. Use `gateway start-runtime`, `detach-runtime`, and `stop-runtime` for relay-owned scheduler-backed services.

## Choose Transport

For a public relay through Cloudflare or another HTTPS edge, use frp with `transport.protocol = "wss"`:

```powershell
$env:CLIO_RELAY_FRP_TOKEN = "<shared-frp-token>"
$env:CLIO_RELAY_STCP_SECRET = "<shared-stcp-secret>"
clio-relay cluster add --name my-cluster --ssh-host my-cluster-login --frp-server-addr relay.example.edu
clio-relay relay-host render-frpc-config --cluster my-cluster --local-port 8848
clio-relay relay-host render-frpc-visitor-config --cluster my-cluster --bind-port 8765
clio-relay relay-host test-http-transport --cluster my-cluster --local-bind-port 18765
```

For closed environments where SSH or VPN already exists, use SSH local forwarding:

```powershell
clio-relay relay-host test-ssh-transport --cluster my-cluster --local-bind-port 18766 --remote-api-port 8766 --session-id relay-ssh-test
```

To leave the remote API alive for a desktop detach and later reattach:

```powershell
clio-relay session start --cluster my-cluster --session-id desktop-session --remote-api-port 8766 --replace
clio-relay session status --cluster my-cluster --session-id desktop-session
clio-relay session detach --cluster my-cluster --session-id desktop-session
clio-relay session teardown --cluster my-cluster --session-id desktop-session
```

Use `session teardown --stop-worker` only when the user chooses to clean up the persistent remote worker too. Teardown keeps relay and scheduler jobs running without prompting. Use `--cancel-jobs` only for an explicit user choice, and add `--cancel-scheduler-jobs` only when the scheduler allocation should also be canceled. The JSON result identifies verified ownership and any residual resources.

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

The tag workflow builds and attests an immutable draft candidate. Independent
maintainer sealing verifies its digest and live reports before promotion
publishes those exact bytes to PyPI. Published-artifact evidence and PyPI
digests must then pass before final verification publishes the GitHub release.
