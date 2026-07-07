# clio-relay

Private relay for running configured cluster work from CLIO without putting application state in the network relay.

`frp` is used only as a byte transport. The frpc-to-frps protocol is configurable: use `wss` for Cloudflare-backed homelab routing now, and switch to `tcp` later when a cloud or institutional relay host provides raw TCP. `clio-core` queue records are the durable state source. JARVIS-CD owns deployment, scheduler submission, provenance, and output collection.

## Roles

- `desktop`: submits work, exposes HTTP/MCP-facing tools, and drains cursors.
- `worker`: leases queued work for a configured cluster, materializes JARVIS-CD runs, and streams events/artifacts back.
- `relay-host`: renders `frps` configuration only. It stores no job state.

## Quickstart

```powershell
uv sync
uv run clio-relay init
uv run clio-relay install-frp
uv run clio-relay cluster bootstrap --cluster ares
$env:CLIO_RELAY_FRP_TOKEN = "<shared-frp-token>"
$env:CLIO_RELAY_STCP_SECRET = "<shared-stcp-secret>"
uv run clio-relay relay-host render-frps-config
uv run clio-relay relay-host render-frpc-config --cluster ares --local-port 8848
uv run clio-relay relay-host render-frpc-visitor-config --cluster ares --bind-port 8765
uv run clio-relay endpoint status
```

On Windows, `install-frp` installs into `.tools/frp/bin`; relay commands auto-discover that project-local binary after checking explicit environment configuration, `PATH`, and bootstrap-managed installs.

For unattended local runs, transport secrets can also live in the ignored `.clio-relay/secrets.json` file:

```json
{
  "CLIO_RELAY_FRP_TOKEN": "<shared-frp-token>",
  "CLIO_RELAY_STCP_SECRET": "<shared-stcp-secret>"
}
```

Submit a JARVIS pipeline intent:

```powershell
uv run clio-relay job submit --cluster ares --jarvis-yaml .\pipeline.yaml
uv run clio-relay job watch <job-id>
uv run clio-relay job read-log <job-id> --stream stdout
uv run clio-relay job list-artifacts <job-id>
```

Expose relay submission tools to an agent process:

```powershell
uv run clio-relay agent render-mcp-config --output .\clio-relay-agent.config.toml
uv run clio-relay agent run --cluster ares --prompt /path/on/cluster/prompt.md --mcp-config /path/on/cluster/clio-relay-agent.config.toml
```

The MCP server provides generic relay tools for JARVIS submission, remote-agent submission, remote MCP-call submission, job state, event cursors, stdout/stderr logs, artifacts, and monitor rules. These tools submit and inspect the same durable `RelayJob` records as the CLI and HTTP surfaces. Workload-specific systems are expressed as JARVIS pipeline YAML, prompt files, or MCP call arguments supplied by the caller, not as relay-native tools.

Job submission is asynchronous by default: submit returns a `job_id`, initial state, kind, and terminal flag. MCP callers can set `wait_for_terminal` with `timeout_seconds` and `poll_seconds` for synchronous submit-and-wait behavior. Monitoring is cursor-based through `relay_monitor_job` or `relay_watch_job_events`; durable task records are available through `job tasks`, HTTP `/jobs/{job_id}/tasks`, and MCP `relay_list_tasks`; stdout and stderr are readable by byte offset through `relay_read_job_log`; artifact references are listed with `relay_list_artifacts` and file artifacts are fetched with `relay_read_artifact`.

Remote agents running inside a single-worker cluster endpoint should submit child cluster work asynchronously, then return the child `job_id` for an outside monitor or later agent turn. A synchronous wait from that same in-flight worker can block the only worker that could execute the child job. Desktop-side agents and HTTP/MCP clients can use synchronous waiting because they are not occupying the cluster worker.

The worker streams JARVIS stdout/stderr into durable events while the process is running (`stdout.delta` and `stderr.delta`) and also writes complete `stdout.log` and `stderr.log` files into the job spool. The clio-core boundary owns job state, event cursors, and artifact metadata; spool files are backing data for logs and artifacts, not the queue.

Every worker run writes and indexes `provenance.json` as a `provenance` artifact. The manifest records the durable job, endpoint, provider settings, materialized pipeline path, stdout/stderr backing files, return code, terminal state, sizes, and hashes. Relay-owned JARVIS packages also emit structured result files into the same job spool. Remote-agent jobs produce `agent-result.json` and, when the configured adapter writes one, `agent-last-message.txt`; MCP-call jobs produce `mcp-result.json` with return code and captured server output. The worker indexes these as `agent_result`, `agent_last_message`, and `mcp_result` artifacts so desktop, HTTP, and MCP clients can inspect result evidence without scraping logs.

Cancellation is durable and cooperative. `job cancel`, HTTP `/jobs/{job_id}/cancel`, and MCP `relay_cancel_job` all record `job.cancel_requested` and move the job to `canceled`. A running worker polls clio-core while JARVIS executes; when it observes cancellation it terminates the JARVIS process group, records `execution.canceled`, and does not overwrite the terminal canceled state.

Worker leases are short lived and renewed while JARVIS is running. If a worker process dies or loses access to clio-core, the next worker loop recovers expired `leased` or `running` jobs for that cluster: jobs below the retry cap are requeued with a `job.requeued` event, and jobs at the cap become `failed` with an explicit `job.failed` event. This prevents orphaned long-running work from remaining invisible while avoiding duplicate execution from healthy workers.

Monitor rules are durable observer records over a job event stream. A regex rule can match event messages or streamed `text` payloads, then emit a `monitor.triggered` event or submit a generic remote-agent task. Rules are cursor-based and one-shot by default after a match, so replay does not duplicate actions.

The HTTP API enforces `CLIO_RELAY_API_TOKEN` when that environment variable is set. Clients can send either `Authorization: Bearer <token>` or `X-Clio-Relay-Token: <token>`. `/healthz` remains unauthenticated for local process checks. When exposing the API through frp or another relay, start it with `clio-relay api start --require-token` so missing API auth fails at startup.

HTTP clients can submit raw `RelayJob` records through `POST /jobs` or use typed submit endpoints: `POST /jobs/jarvis`, `POST /jobs/remote-agent`, and `POST /jobs/mcp-call`. The typed endpoints preserve the same durable queue semantics while keeping provider/workload details in request payloads instead of relay-native hardcoding.

Run a full configured live acceptance:

```powershell
uv run clio-relay live-test --cluster ares --jarvis-yaml .\.clio-relay\live\ares-lammps.yaml --monitor-pattern "Total.*wall.*time"
```

`live-test` does not contain workload recipes. It takes acceptance inputs from CLI options or from the cluster registry's `live_test` object:

```json
{
  "live_test": {
    "jarvis_yaml": ".clio-relay/live/ares-lammps.yaml",
    "monitor_pattern": "Total.*wall.*time",
    "progress_pattern": "^\\s*(?P<step>\\d+)\\s+",
    "progress_action_payload": {
      "label": "iteration",
      "current_group": "step",
      "total": 150,
      "unit": "step"
    },
    "agent_prompt": "/home/user/.local/share/clio-relay/agent-tests/prompt.md",
    "agent_mcp_config": "/home/user/.local/share/clio-relay/agent-tests/mcp.toml"
  }
}
```

The acceptance runner submits the configured JARVIS YAML on the target cluster, waits for terminal success, verifies event replay, verifies durable task records, reads stdout/stderr by offset, lists and reads artifacts, verifies the provenance artifact, evaluates the configured monitor pattern, and can record structured progress from stdout events through a configured regex/action payload. When an agent MCP config is supplied, acceptance also requires the agent to report a child relay job id and verifies that child job with the same event, task, log, artifact, and provenance checks. The cluster registry owns what `ares`, `homelab`, or any later target means.

## Cloudflare-backed frps edge

For homelab deployments behind Cloudflare Tunnel, publish `frps.jcernuda.com` to an HTTP origin such as `http://localhost:7000` and run nginx or another HTTP reverse proxy at that origin. The proxy should forward WebSocket requests for frp's default control path `/~!frp` to a loopback-only `frps` listener, for example `127.0.0.1:7001`.

Endpoints then use `frpc` with `transport.protocol = "wss"` and `serverPort = 443`. The cluster-side frpc config exposes a loopback API as an STCP proxy; the desktop-side visitor config binds a local desktop port to that proxy through the same frps. This keeps client setup to normal relay config and leaves the Cloudflare-specific routing in homelab infrastructure. If a later relay host supports raw TCP directly, change the configured transport to `tcp` without changing queue, job, agent, or cluster semantics.
