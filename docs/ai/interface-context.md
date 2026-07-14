# Interface Context

This file is a dense map for coding agents. Keep the README shorter than this file.

## CLI Surfaces

Core setup:

- `clio-relay init`
- `clio-relay install-frp`
- `clio-relay cluster add`
- `clio-relay cluster bootstrap`
- `clio-relay cluster install-endpoint-service`
- `clio-relay doctor`
- `clio-relay live-test`
- `clio-relay release validate-local`
- `clio-relay release gate`

Transport and sessions:

- `clio-relay relay-host render-frps-config`
- `clio-relay relay-host render-frpc-config`
- `clio-relay relay-host render-frpc-visitor-config`
- `clio-relay relay-host test-frpc-connection`
- `clio-relay relay-host test-http-transport`
- `clio-relay relay-host test-direct-transport`
- `clio-relay relay-host test-ssh-transport`
- `clio-relay session start`
- `clio-relay session status`
- `clio-relay session teardown`

Endpoint and job work:

- `clio-relay endpoint start`
- `clio-relay endpoint status`
- `clio-relay queue list`
- `clio-relay queue migration-status`
- `clio-relay queue migrate-indexes`
- `clio-relay queue diagnose <job-id>`
- `clio-relay queue stale`
- `clio-relay queue cleanup-stale`
- `clio-relay queue cancel <job-id>`
- `clio-relay queue validate <expendable-job-id>`
- `clio-relay queue retention-plan <job-id>`
- `clio-relay queue retention-status <job-id>`
- `clio-relay queue retention-collect <job-id>`
- `clio-relay storage status`
- `clio-relay worker status`
- `clio-relay job submit`
- `clio-relay job watch`
- `clio-relay job cancel`
- `clio-relay job tasks`
- `clio-relay job task-events`
- `clio-relay job record-task-event`
- `clio-relay job read-log`
- `clio-relay job list-artifacts`
- `clio-relay job read-artifact`
- `clio-relay job progress`
- `clio-relay gateway create`
- `clio-relay gateway list`
- `clio-relay gateway get`
- `clio-relay gateway update`
- `clio-relay gateway close`

Agent and monitor work:

- `clio-relay agent render-mcp-config`
- `clio-relay agent run`
- `clio-relay remote-mcp register`
- `clio-relay remote-mcp unregister`
- `clio-relay remote-mcp list`
- `clio-relay remote-mcp refresh`
- `clio-relay remote-mcp reload`
- `clio-relay remote-mcp validate`
- `clio-relay monitor add-regex`
- `clio-relay monitor run-once`

## HTTP Surfaces

The HTTP API exposes:

- health check
- job submission
- typed JARVIS submission
- typed remote-agent submission
- typed MCP-call submission
- job state
- job events
- task records
- task timeline event reads and writes
- task timeline SSE and WebSocket streams
- stdout and stderr reads by offset
- artifact listing and reads
- progress reads
- gateway session create, list, read, update, and close
- cancellation
- bounded queue listing, exact-job diagnosis, stale discovery and cleanup
- worker capacity and per-job-kind concurrency status

Queue routes are `GET /queue`, `GET /queue/jobs/{job_id}/diagnose`,
`GET /queue/stale`, `POST /queue/cleanup-stale`,
`POST /queue/jobs/{job_id}/cancel`, and `GET /workers`. Job-specific routes
accept a cluster assertion; global stale inspection and mutation are denied to
owner-session-scoped APIs.

When `CLIO_RELAY_API_TOKEN` is set and the API is started with `--require-token`, clients must send either `Authorization: Bearer <token>` or `X-Clio-Relay-Token: <token>`. `/healthz` stays open for local process checks.

## MCP Surfaces

The MCP server exposes relay tools for:

- submit JARVIS pipeline
- submit remote agent task
- submit remote MCP call
- monitor job
- watch event cursors
- list task records
- record and watch task timeline events
- read logs
- list and read artifacts
- record and list progress
- create, read, update, and close gateway sessions
- create monitor rules
- cancel jobs
- list the bounded relay queue
- diagnose one relay job with queue, lease, worker, scheduler, event, and progress evidence
- discover stale jobs without mutation
- clean stale jobs from the admin profile with dry-run and relay-only defaults

MCP tools operate on the same durable records as CLI and HTTP calls.

Task, artifact, and progress collections use exact one-based `cursor`, `limit`,
`next_cursor`, and `total` fields. Global job, endpoint, gateway, and monitor-rule
filters apply inside a durable source window and therefore use `source_cursor`,
`source_limit`, `source_next_cursor`, and `source_total`. A filtered global page
can be empty while `source_next_cursor` remains non-null. Limits default to 100
and never exceed 500.

Storage admission failures are machine-readable: HTTP returns status 507 with a
`clio-relay.storage-decision.v1` detail, CLI prints a stable JSON refusal, and MCP
returns the same decision in JSON-RPC error data. Terminal retention is dry-run by
default, never requests scheduler cancellation, and mutation is available only in
the administrative MCP profile.

## Environment and Config

Important environment variables:

- `CLIO_RELAY_CORE_DIR`
- `CLIO_RELAY_SPOOL_DIR`
- `CLIO_RELAY_API_TOKEN`
- `CLIO_RELAY_FRP_TOKEN`
- `CLIO_RELAY_STCP_SECRET`
- `CLIO_RELAY_JARVIS_BIN`
- `CLIO_RELAY_FRPC_BIN`
- `CLIO_RELAY_AGENT_BIN`
- `CLIO_RELAY_AGENT_ADAPTER`
- `CLIO_RELAY_AGENT_ARGS`
- `CLIO_RELAY_CLI_MODE`
- `CLIO_RELAY_REMOTE_MCP_CACHE`

Local cluster registry data lives under `.clio-relay/clusters.json` by default. Secrets for unattended local runs can live in ignored `.clio-relay/secrets.json`.

Remote MCP registrations are cluster-scoped entries in the cluster registry.
Their discovered schemas live in `.clio-relay/remote-mcp-cache.json` by default.
The MCP server reloads both files for every `tools/list`; only the explicit
`remote-mcp refresh` command performs durable cluster-side discovery.

## Live Examples Are Not Product Defaults

Live targets may use Ares, Codex, Cloudflare-backed frp, and external JARVIS
application packages. Treat them as tested configurations, not fixed product
semantics. Application installers and progress parsers are external plugins,
not relay defaults.
