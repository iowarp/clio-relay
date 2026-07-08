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

MCP tools operate on the same durable records as CLI and HTTP calls.

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

Local cluster registry data lives under `.clio-relay/clusters.json` by default. Secrets for unattended local runs can live in ignored `.clio-relay/secrets.json`.

## Live Examples Are Not Product Defaults

The current live target uses Ares, Codex, Cloudflare-backed frp, and JARVIS builtin LAMMPS examples. Treat those as tested configurations, not fixed product semantics.
