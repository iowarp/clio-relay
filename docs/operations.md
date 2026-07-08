# operations

This page covers the common operator paths. Use the README for the short overview and `docs/ai/` for the full implementation context.

## add a cluster

```powershell
uv run clio-relay cluster add --name my-cluster --ssh-host my-cluster-login --agent-adapter exec --agent-bin agent
uv run clio-relay cluster bootstrap --cluster my-cluster
uv run clio-relay cluster install-endpoint-service --cluster my-cluster --start --enable
```

Cluster names are local labels. `ares`, `homelab`, or a later institutional target are registry entries, not hardcoded behavior.

## run a job

```powershell
uv run clio-relay job submit --cluster my-cluster --jarvis-yaml .\pipeline.yaml
uv run clio-relay job watch <job-id> --cluster my-cluster
uv run clio-relay job read-log <job-id> --cluster my-cluster --stream stdout
uv run clio-relay job list-artifacts <job-id> --cluster my-cluster
```

Submissions are asynchronous by default. CLI, HTTP, and MCP callers get a `job_id` and can monitor events and logs by cursor or byte offset.

## expose tools to an agent

```powershell
uv run clio-relay agent render-mcp-config --output .\clio-relay-agent.config.toml
uv run clio-relay agent run --cluster my-cluster --prompt /path/on/cluster/prompt.md --mcp-config /path/on/cluster/clio-relay-agent.config.toml
```

Agents should submit child cluster work asynchronously and return the child `job_id`. A single cluster worker cannot execute a child job while it is blocked inside a parent agent job waiting for that child to finish.

Agents can also record structured task timeline events:

```powershell
uv run clio-relay job record-task-event <task-id> --cluster my-cluster --event-type dataset_found --label dataset --summary "found staged dataset" --path-ref /mnt/common/datasets/red_sea_001
uv run clio-relay job task-events <task-id> --cluster my-cluster --cursor 1
```

Use timeline events for UI-visible agent work such as repository scans, dataset discovery, generated scripts, planned commands, scheduler submissions, warnings, and completion. Use normal job logs for stdout and stderr.

## manage visualization gateways

Use gateway sessions for scheduler-backed services that need to survive long enough for a desktop to connect, such as ParaView or another cluster-side visualizer.

```powershell
uv run clio-relay gateway create --cluster my-cluster --name paraview-red-sea --gateway-json-file .\gateway.json
uv run clio-relay gateway update <session-id> --cluster my-cluster --state ready --scheduler-job-id 12345 --node compute-01 --gateway-json-file .\gateway-ready.json
uv run clio-relay gateway get <session-id> --cluster my-cluster
uv run clio-relay gateway close <session-id> --cluster my-cluster
```

`close` marks the durable session closed. The scheduler or package path should still clean up the actual service process or scheduler job it owns.

## use frp transport

Use frp when the desktop and cluster cannot directly SSH to each other but can both reach a relay host.

```powershell
$env:CLIO_RELAY_FRP_TOKEN = "<shared-frp-token>"
$env:CLIO_RELAY_STCP_SECRET = "<shared-stcp-secret>"
uv run clio-relay cluster add --name my-cluster --ssh-host my-cluster-login --frp-server-addr relay.example.edu
uv run clio-relay relay-host render-frpc-config --cluster my-cluster --local-port 8848
uv run clio-relay relay-host render-frpc-visitor-config --cluster my-cluster --bind-port 8765
uv run clio-relay relay-host test-http-transport --cluster my-cluster --local-bind-port 18765
```

For Cloudflare-backed homelab deployments, configure the cluster transport as `wss` over port `443`. For a raw public relay host, configure `tcp`.

## use ssh forwarding

Use SSH forwarding when the desktop already has SSH or VPN access to the cluster.

```powershell
uv run clio-relay relay-host test-ssh-transport --cluster my-cluster --local-bind-port 18766 --remote-api-port 8766 --session-id relay-ssh-test
```

For detach and reattach workflows:

```powershell
uv run clio-relay session start --cluster my-cluster --session-id desktop-session --remote-api-port 8766 --replace
uv run clio-relay session status --cluster my-cluster --session-id desktop-session
uv run clio-relay session teardown --cluster my-cluster --session-id desktop-session
```

To clean up the persistent worker too:

```powershell
uv run clio-relay session teardown --cluster my-cluster --session-id desktop-session --stop-worker
```

## live acceptance

```powershell
uv run clio-relay live-test --cluster ares --jarvis-yaml .\examples\ares-lammps\pipeline.yaml --monitor-pattern "Loop time"
```

A complete live acceptance should verify the cluster bootstrap, worker service, transport, JARVIS package execution, logs, artifacts, provenance, progress, and agent tool submission path.
