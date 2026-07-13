# Connect a desktop, homelab relay, and cluster

This guide starts from three machines that are not already connected through
`clio-relay`:

- a local desktop that owns the user request and local `clio-relay` config
- a homelab or public host that runs `frps` as a dumb relay endpoint
- a cluster login node that can run the `clio-relay` worker, JARVIS, and the
  configured agent binary

The homelab relay does not store job state. It only joins outbound `frp`
connections from the desktop and the cluster.

Install the released relay as a persistent tool on each operator host that runs
`clio-relay`. Replace `<released-version>` with the exact version being deployed:

```bash
uv tool install --python 3.12 --no-config "clio-relay==<released-version>"
```

The commands below use that persistent executable. `uvx` is intentionally not
used for a long-lived relay deployment because it creates a temporary execution
environment rather than the independently managed tool installation recorded by
the release evidence.

## Start the relay host

On the relay host, run `frps` behind a public endpoint. For Cloudflare-backed
deployments, use WebSocket transport on port `443`. For a raw public host, use
the configured TCP port.

```bash
clio-relay relay-host render-frps-config \
  --bind-port 7000 \
  --vhost-http-port 8080 \
  --auth-token "$CLIO_RELAY_FRP_TOKEN" \
  > frps.toml

frps -c frps.toml
```

Both the desktop and the cluster must be able to reach this public relay
endpoint.

## Configure the desktop

Set the shared relay secrets in the desktop shell:

```powershell
$env:CLIO_RELAY_FRP_TOKEN = "<shared-frp-token>"
$env:CLIO_RELAY_STCP_SECRET = "<shared-stcp-secret>"
```

Add the cluster. The cluster name, relay endpoint, and agent binary are local
configuration values.

```powershell
clio-relay cluster add `
  --name my-cluster `
  --ssh-host my-cluster-login `
  --frp-server-addr relay.example.org `
  --frp-server-port 443 `
  --frp-protocol wss `
  --agent-adapter exec `
  --agent-bin /home/<user>/.local/bin/agent
```

For Codex today, `--agent-bin` can point at the Codex executable. Later it can
point at `clio`, Claude, or another adapter. Do not bake a provider-specific
agent name into prompts, packages, or workflow code.

## Bootstrap the cluster

From the desktop:

```powershell
clio-relay cluster bootstrap --cluster my-cluster
```

Install and start the cluster worker as a user-level service:

```powershell
clio-relay cluster install-endpoint-service `
  --cluster my-cluster `
  --start `
  --enable
```

The worker reads queued jobs from the relay core and runs them through JARVIS.
It does not require sudo.

## Expose relay tools to the remote agent

Render an MCP config that exposes the relay tools:

```powershell
clio-relay agent render-mcp-config `
  --output .\clio-relay-agent.config.toml
```

Copy the MCP config and prompt to the cluster:

```powershell
scp .\clio-relay-agent.config.toml my-cluster-login:/home/<user>/relay/clio-relay-agent.config.toml
scp .\prompt.md my-cluster-login:/home/<user>/relay/prompt.md
```

The prompt should tell the agent to use the relay tools rather than bypassing
the relay. For example:

```text
Use the clio-relay MCP tools. Submit the requested runtime or JARVIS pipeline
through clio-relay. Return the child job id or gateway session id. Do not run
the workload directly outside clio-relay.
```

## Submit a remote agent run

From the desktop:

```powershell
clio-relay agent run `
  --cluster my-cluster `
  --prompt /home/<user>/relay/prompt.md `
  --mcp-config /home/<user>/relay/clio-relay-agent.config.toml `
  --idempotency-key desktop-agent-run-001
```

This creates a `remote_agent` relay job. The cluster worker picks it up, JARVIS
launches the configured agent binary, and the agent runs on the cluster with the
relay MCP tools available.

Monitor the parent agent job:

```powershell
clio-relay job watch <agent-job-id> --cluster my-cluster

clio-relay job read-log <agent-job-id> `
  --cluster my-cluster `
  --stream stdout

clio-relay job list-artifacts <agent-job-id> --cluster my-cluster
```

If the agent submits child work, it should return the child `job_id` or gateway
session id. Monitor the child separately.

## Connect to a live service

For scheduler-backed services, use a managed runtime. The runtime starts the
application on a compute node and connects it to the desktop through `frp`.

The data path is:

```text
application service on cluster compute node
  -> cluster-side frpc outbound to relay host
  -> frps relay host
  -> desktop-side frpc visitor
  -> http://127.0.0.1:<desktop-port>/<stream-path>
```

The stream is pushed over the live transport. The relay core stores session,
job, scheduler, lifecycle, and artifact metadata. It does not store bulk image
or data stream frames unless the application or JARVIS package also writes them
as artifacts.

## Detach or clean up

To close only the relay connectors while keeping the remote scheduler job alive:

```powershell
clio-relay gateway stop-runtime <session-id> `
  --cluster my-cluster `
  --keep-scheduler-job
```

To explicitly stop the remote scheduler job:

```powershell
clio-relay gateway stop-runtime <session-id> `
  --cluster my-cluster `
  --cancel-scheduler-job
```

The default desktop behavior should be detach and keep running. Cancel the
remote job only when the user explicitly asks for that.
