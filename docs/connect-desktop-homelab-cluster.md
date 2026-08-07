# Connect a desktop, homelab relay, and cluster

This guide starts from three machines that are not already connected through
`clio-relay`:

- a local desktop that owns the user request and local `clio-relay` config
- a homelab or public host that runs `frps` as a dumb relay endpoint
- a cluster login node that can run the `clio-relay` worker, JARVIS, and the
  configured agent binary

The homelab relay does not store job state. It only joins outbound `frp`
connections from the desktop and the cluster.

This walkthrough builds one connection: the desktop-local relay and the
cluster-side relay joined by exactly one persistent link, which then carries
every later operation. `docs/connection-model.md` is the normative statement of
that model — the transport modes, the ssh budget, reconnect, and how run inputs
reach the cluster. Read it before adapting these steps into automation.

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

Place the MCP config and prompt on the cluster. This is **bootstrap-time file
placement**: it happens once, at bring-up, alongside the deploy step that the
mode's ssh budget already covers, because `agent run` takes cluster-side paths
for both and relay has no channel for placing them today. Any file placement
mechanism the site already trusts is acceptable here; the point is that it
happens once, before the connection carries work:

```powershell
scp .\clio-relay-agent.config.toml my-cluster-login:/home/<user>/relay/clio-relay-agent.config.toml
scp .\prompt.md my-cluster-login:/home/<user>/relay/prompt.md
```

This step is not a pattern to generalize. It does not repeat per run, per job,
or per operation, and it never carries run inputs. A file that a job reads —
a simulation input deck, a script a package executes, a dataset descriptor —
reaches the cluster through relay's input staging, described under
"Move a run input to the cluster" below. Copying such a file out of band leaves
the job with an empty `used_artifact_refs` and no lineage, and the run stops
being reproducible even when it succeeds.

The prompt should tell the agent to use the relay tools rather than bypassing
the relay. For example:

```text
Use the clio-relay MCP tools. Submit the requested runtime or JARVIS pipeline
through clio-relay. Return the child job id or gateway session id. Do not run
the workload directly outside clio-relay. Do not use ssh, scp, or rsync to move
files or run commands on the cluster; relay staging is the only path for run
inputs.
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

## Move a run input to the cluster

Files that a job reads are staged by the relay over the connection's one link.
Nothing is copied to the cluster by hand after bootstrap.

Point the desktop MCP process at the workspace it may read, on the desktop:

```powershell
$env:CLIO_RELAY_INPUT_WORKSPACE_ROOT = "$PWD\my-workspace"
```

Then configure the step with the path **relative to that workspace**, not a
cluster path:

```text
jarvis_add_step(cluster="my-cluster", pipeline_id=..., step_id=...,
                config={"script": "inputs/run.in"}, wait_for_terminal=true)
```

Staging is authorized by the package's own schema: the setting must declare
`jarvis.configuration-input-binding.v1` with `kind="local_file"`, discovered
through `jarvis_describe(target="package", ...)` on the same connection. Relay
snapshots the bytes from the desktop workspace, hashes them, ingests them
through the owned session, materializes a content-addressed copy on the cluster,
rewrites the setting to that staged reference, and records lineage.

Verify it engaged before trusting the run:

```powershell
clio-relay job used-artifacts <job-id> --cluster my-cluster
```

A non-empty result whose digest matches the desktop file is the proof. An empty
result on a package that declared a file-typed setting means the bytes never
travelled through relay: the run has no lineage and is not reproducible. Fix the
staging path rather than copying the file to the cluster.

Settings that declare no binding are passed through unchanged. A path-looking
value is never enough on its own to make relay read a local file, so a
cluster-absolute path in that slot is forwarded verbatim and stages nothing.

## Connect to a live service

For scheduler-backed services, use a managed runtime. The runtime starts the
application on a compute node and connects it to the desktop through `frp`.

The data path is the same as every other kind of traffic — the compute node
talks to the cluster relay over cluster-internal connectivity, and the cluster
relay carries the stream over the connection's one link:

```text
application service on cluster compute node
  -> cluster relay on the master node (cluster-internal connectivity)
  -> the connection's link (relay point TCP, or the held ssh forward)
  -> local relay
  -> http://127.0.0.1:<desktop-port>/<stream-path>
```

A compute node never opens transport of its own: it needs no outbound internet
reachability, and the stream's semantics are identical in every transport mode.
(The current implementation deviates — it launches a compute-node-side `frpc`
that dials the relay host directly, which both bypasses the cluster relay and
demands outbound internet access from compute nodes; tracked on the
connection-model restoration, issue #179.)

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
