# operations

This page covers the common operator paths. Use the README for the short overview and `docs/ai/` for the full implementation context.

## add a cluster

```powershell
uv run clio-relay cluster add --name ares --ssh-host ares --agent-adapter codex --agent-npm-package @openai/codex --agent-npm-bin codex
uv run clio-relay cluster bootstrap --cluster ares
uv run clio-relay cluster install-endpoint-service --cluster ares --start --enable
```

Cluster names are local labels. `ares`, `homelab`, or a later institutional target are registry entries, not hardcoded behavior.

## run a job

```powershell
uv run clio-relay job submit --cluster ares --jarvis-yaml .\examples\ares-lammps\pipeline.yaml
uv run clio-relay job watch <job-id> --cluster ares
uv run clio-relay job read-log <job-id> --cluster ares --stream stdout
uv run clio-relay job list-artifacts <job-id> --cluster ares
```

Submissions are asynchronous by default. CLI, HTTP, and MCP callers get a `job_id` and can monitor events and logs by cursor or byte offset.

## expose tools to an agent

```powershell
uv run clio-relay agent render-mcp-config --output .\clio-relay-agent.config.toml
uv run clio-relay agent run --cluster ares --prompt /path/on/cluster/prompt.md --mcp-config /path/on/cluster/clio-relay-agent.config.toml
```

Agents should submit child cluster work asynchronously and return the child `job_id`. A single cluster worker cannot execute a child job while it is blocked inside a parent agent job waiting for that child to finish.

## use frp transport

Use frp when the desktop and cluster cannot directly SSH to each other but can both reach a relay host.

```powershell
$env:CLIO_RELAY_FRP_TOKEN = "<shared-frp-token>"
$env:CLIO_RELAY_STCP_SECRET = "<shared-stcp-secret>"
uv run clio-relay relay-host render-frpc-config --cluster ares --local-port 8848
uv run clio-relay relay-host render-frpc-visitor-config --cluster ares --bind-port 8765
uv run clio-relay relay-host test-http-transport --cluster ares --local-bind-port 18765
```

For Cloudflare-backed homelab deployments, configure the cluster transport as `wss` over port `443`. For a raw public relay host, configure `tcp`.

## use ssh forwarding

Use SSH forwarding when the desktop already has SSH or VPN access to the cluster.

```powershell
uv run clio-relay relay-host test-ssh-transport --cluster ares --local-bind-port 18766 --remote-api-port 8766 --session-id relay-ssh-test
```

For detach and reattach workflows:

```powershell
uv run clio-relay session start --cluster ares --session-id desktop-session --remote-api-port 8766 --replace
uv run clio-relay session status --cluster ares --session-id desktop-session
uv run clio-relay session teardown --cluster ares --session-id desktop-session
```

To clean up the persistent worker too:

```powershell
uv run clio-relay session teardown --cluster ares --session-id desktop-session --stop-worker
```

## live acceptance

```powershell
uv run clio-relay live-test --cluster ares --jarvis-yaml .\examples\ares-lammps\pipeline.yaml --monitor-pattern "Loop time"
```

A complete live acceptance should verify the cluster bootstrap, worker service, transport, JARVIS package execution, logs, artifacts, provenance, progress, and agent tool submission path.
