# Operations

This page covers the common operator paths. Use the README for the short overview and `docs/ai/` for the full implementation context.

For a complete first-connection walkthrough from a local desktop through a
homelab relay to a cluster worker and remote agent, see
`docs/connect-desktop-homelab-cluster.md`.

## Add a Cluster

```powershell
uvx --python 3.12 --from clio-relay clio-relay cluster add --name my-cluster --ssh-host my-cluster-login --agent-adapter exec --agent-bin agent
uvx --python 3.12 --from clio-relay clio-relay cluster bootstrap --cluster my-cluster
uvx --python 3.12 --from clio-relay clio-relay cluster install-endpoint-service --cluster my-cluster --start --enable
```

Cluster names are local labels. `ares`, `homelab`, or a later institutional target are registry entries, not hardcoded behavior.

## Run a Job

```powershell
uvx --python 3.12 --from clio-relay clio-relay job submit --cluster my-cluster --jarvis-yaml .\pipeline.yaml
uvx --python 3.12 --from clio-relay clio-relay job watch <job-id> --cluster my-cluster
uvx --python 3.12 --from clio-relay clio-relay job read-log <job-id> --cluster my-cluster --stream stdout
uvx --python 3.12 --from clio-relay clio-relay job list-artifacts <job-id> --cluster my-cluster
```

Submissions are asynchronous by default. CLI, HTTP, and MCP callers get a `job_id` and can monitor events and logs by cursor or byte offset.

## Manage the Relay Queue

Relay queue state is separate from cluster scheduler state. A queued relay job
has not been leased by a worker yet. A running relay job may already have
submitted scheduler work through its JARVIS package.

```powershell
uvx --python 3.12 --from clio-relay clio-relay queue list --cluster my-cluster
uvx --python 3.12 --from clio-relay clio-relay queue diagnose --cluster my-cluster
uvx --python 3.12 --from clio-relay clio-relay queue cleanup-stale --cluster my-cluster --dry-run
uvx --python 3.12 --from clio-relay clio-relay worker status --cluster my-cluster
```

Cancel queued jobs without touching any scheduler:

```powershell
uvx --python 3.12 --from clio-relay clio-relay queue cancel <job-id> --cluster my-cluster
```

For leased or running jobs, scheduler cancellation is explicit:

```powershell
uvx --python 3.12 --from clio-relay clio-relay queue cancel <job-id> --cluster my-cluster --cancel-scheduler-job
```

Use `--keep-scheduler-job` or omit the flag when the user only wants the relay
job/session to stop observing or driving the work. Use `--cancel-scheduler-job`
only when the user explicitly wants the package or scheduler adapter to stop the
remote scheduler job too.

Worker capacity is configured when the user-level worker service is installed:

```powershell
uvx --python 3.12 --from clio-relay clio-relay cluster install-endpoint-service --cluster my-cluster --concurrency 4 --start --enable
```

This keeps one sudo-less user service per cluster and runs multiple in-process
worker slots inside that service.

## Expose Tools to an Agent

```powershell
uvx --python 3.12 --from clio-relay clio-relay agent render-mcp-config --output .\clio-relay-agent.config.toml
uvx --python 3.12 --from clio-relay clio-relay agent run --cluster my-cluster --prompt /path/on/cluster/prompt.md --mcp-config /path/on/cluster/clio-relay-agent.config.toml
```

Agents should submit child cluster work asynchronously and return the child `job_id`. A single cluster worker cannot execute a child job while it is blocked inside a parent agent job waiting for that child to finish.

Agents can also record structured task timeline events:

```powershell
uvx --python 3.12 --from clio-relay clio-relay job record-task-event <task-id> --cluster my-cluster --event-type dataset_found --label dataset --summary "found staged dataset" --path-ref /mnt/common/datasets/example_001
uvx --python 3.12 --from clio-relay clio-relay job task-events <task-id> --cluster my-cluster --cursor 1
```

Use timeline events for UI-visible agent work such as repository scans, dataset discovery, generated scripts, planned commands, scheduler submissions, warnings, and completion. Use normal job logs for stdout and stderr.

## Use Remote JARVIS MCP

The relay can run the JARVIS MCP server inside the target cluster environment through:

```bash
uvx --from clio-kit==2.2.6 clio-kit mcp-server jarvis
```

The default agent MCP profile exposes compact relay tools and the compact JARVIS tools:

- `relay_submit_agent`
- `relay_status`
- `relay_cancel`
- `relay_observe`
- `relay_wait`
- `jarvis_create_pipeline`
- `jarvis_describe`
- `jarvis_add_step`
- `jarvis_edit_step`
- `jarvis_remove_step`
- `jarvis_run`

The expected workflow is to create or load a pipeline through those JARVIS tools, use `jarvis_describe` for package and pipeline inspection, and call `jarvis_run` to submit the configured pipeline through the cluster-local JARVIS environment. `relay_observe` and `relay_wait` are the agent-facing monitor loop for progress, stdout, stderr, and terminal output.

For prerelease testing, set `CLIO_RELAY_JARVIS_MCP_COMMAND` to a JSON string array on the worker environment. The command is interpreted on the cluster, so it can point at a PyPI release, a Git branch, or a site-local executable.

Operational queue, gateway-session, raw MCP-call, and low-level log tools are available through:

```bash
clio-relay mcp-server --profile admin
```

See `docs/remote-mcp-federation.md` for the full agent-facing model.

## Manage Streaming Service Runtimes

Use gateway sessions for scheduler-backed services that need to survive long enough for a desktop to connect, such as a visualization service, Jupyter-like service, remote MCP server, or long-running agent service.

For a managed runtime, describe the application generically with `ServiceRuntimeSpec`. In production, `submit_command` should invoke a JARVIS package or pipeline. That package owns the application launch, scheduler script, readiness behavior, logs, provenance, and any application-specific stream protocol. The relay waits for the allocated node and service health, starts the cluster-side frp connector, starts the desktop visitor, records both owned PIDs/configs/logs, and returns desktop-local URLs.

```json
{
  "kind": "streaming-http-service",
  "deployment_driver": "jarvis",
  "submit_command": [
    "jarvis",
    "run",
    "/remote/service-runtime.yaml",
    "--set",
    "RELAY_APPLICATION_PORT=18777"
  ],
  "status_command": [
    "jarvis",
    "runtime",
    "status",
    "{scheduler_job_id}"
  ],
  "cancel_command": [
    "jarvis",
    "runtime",
    "cancel",
    "{scheduler_job_id}"
  ],
  "service_port": 18777,
  "health_path": "/healthz",
  "stream_mode": "push",
  "stream_path": "/live-data",
  "event_stream_path": "/events",
  "state_path": "/state",
  "compatibility_paths": {
    "snapshot": "/debug/snapshot"
  },
  "desktop_bind_addr": "127.0.0.1",
  "desktop_bind_port": 28777,
  "proxy_name": "my-service-session",
  "transport_mode": "frp-stcp-wss",
  "readiness_timeout_seconds": 900,
  "poll_seconds": 5,
  "scheduler": "external",
  "connect_url_template": "http://{bind_addr}:{bind_port}"
}
```

```powershell
uvx --python 3.12 --from clio-relay clio-relay gateway start-runtime --cluster my-cluster --name my-live-service --runtime-json-file .\runtime.json
uvx --python 3.12 --from clio-relay clio-relay gateway get <session-id>
uvx --python 3.12 --from clio-relay clio-relay gateway stop-runtime <session-id> --cluster my-cluster --keep-scheduler-job
```

Use `transport_mode: "frp-stcp-wss"` for the relay path and `transport_mode: "frp-xtcp-wss"` for direct NAT-bypass attempts. The application stream still flows through the relay/bypass transport to the desktop-local bind port, not through an SSH port forward.

The default service contract is push-based. A desktop client subscribes once to `stream_url`, and the remote application pushes data, images, frames, or domain records as they are emitted. The relay does not assume any application-specific endpoint shape. Pull-style endpoints are represented only as named `compatibility_paths`, such as `snapshot`, `render_once`, or `state_dump`.

The JARVIS package must emit structured JSON records for the runtime supervisor. The submit command must eventually print a JSON object with `scheduler_job_id` and may include `service_host` when allocation is already known:

```json
{"scheduler_job_id":"12345","service_host":"compute-01"}
```

If `service_host` is not known at submission time, provide `status_command`. The status command must print JSON such as:

```json
{"state":"allocated","service_host":"compute-01","reason":null,"events":[{"type":"progress","source":"jarvis_package","package":"example_stream","message":"runtime allocated"}]}
```

The JARVIS package is the source of application monitoring. It can watch stdout, stderr, scheduler logs, readiness files, or application-specific control channels, then report generic structured events through its status output or through the service's push stream. Scheduler-specific and application-specific parsing belongs in the JARVIS package or scheduler adapter. The service runtime supervisor does not parse scheduler command output, scheduler environment variables, or application logs.

`stop-runtime` stops owned relay connector processes. It keeps the scheduler job by default; pass `--cancel-scheduler-job` only when the user explicitly wants to stop the remote application.

Manual gateway record updates are still available for package integrations and external supervisors:

```powershell
uvx --python 3.12 --from clio-relay clio-relay gateway create --cluster my-cluster --name live-service-example --gateway-json-file .\gateway.json
uvx --python 3.12 --from clio-relay clio-relay gateway update <session-id> --cluster my-cluster --state ready --scheduler-job-id 12345 --node compute-01 --gateway-json-file .\gateway-ready.json
uvx --python 3.12 --from clio-relay clio-relay gateway get <session-id> --cluster my-cluster
uvx --python 3.12 --from clio-relay clio-relay gateway close <session-id> --cluster my-cluster
```

`close` only marks the durable record closed. Use `stop-runtime` when clio-relay owns the connector lifecycle.

## Use FRP Transport

Use frp when the desktop and cluster cannot directly SSH to each other but can both reach a relay host.

```powershell
$env:CLIO_RELAY_FRP_TOKEN = "<shared-frp-token>"
$env:CLIO_RELAY_STCP_SECRET = "<shared-stcp-secret>"
uvx --python 3.12 --from clio-relay clio-relay cluster add --name my-cluster --ssh-host my-cluster-login --frp-server-addr relay.example.edu
uvx --python 3.12 --from clio-relay clio-relay relay-host render-frpc-config --cluster my-cluster --local-port 8848
uvx --python 3.12 --from clio-relay clio-relay relay-host render-frpc-visitor-config --cluster my-cluster --bind-port 8765
uvx --python 3.12 --from clio-relay clio-relay relay-host test-http-transport --cluster my-cluster --local-bind-port 18765
```

For Cloudflare-backed homelab deployments, configure the cluster transport as `wss` over port `443`. For a raw public relay host, configure `tcp`.

## Use SSH Forwarding

Use SSH forwarding when the desktop already has SSH or VPN access to the cluster.

```powershell
uvx --python 3.12 --from clio-relay clio-relay relay-host test-ssh-transport --cluster my-cluster --local-bind-port 18766 --remote-api-port 8766 --session-id relay-ssh-test
```

For detach and reattach workflows:

```powershell
uvx --python 3.12 --from clio-relay clio-relay session start --cluster my-cluster --session-id desktop-session --remote-api-port 8766 --replace
uvx --python 3.12 --from clio-relay clio-relay session status --cluster my-cluster --session-id desktop-session
uvx --python 3.12 --from clio-relay clio-relay session teardown --cluster my-cluster --session-id desktop-session
```

To clean up the persistent worker too:

```powershell
uvx --python 3.12 --from clio-relay clio-relay session teardown --cluster my-cluster --session-id desktop-session --stop-worker
```

## Live Acceptance

```powershell
uvx --python 3.12 --from clio-relay clio-relay live-test --cluster ares --jarvis-yaml .\examples\ares-lammps\pipeline.yaml --monitor-pattern "Loop time"
```

A complete live acceptance should verify the cluster bootstrap, worker service, transport, JARVIS package execution, logs, artifacts, provenance, progress, and agent tool submission path.
