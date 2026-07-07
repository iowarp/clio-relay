# interface context

This file is a dense map for coding agents. Keep the README shorter than this file.

## cli surfaces

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
- `clio-relay job read-log`
- `clio-relay job list-artifacts`
- `clio-relay job read-artifact`
- `clio-relay job progress`

Agent and monitor work:

- `clio-relay agent render-mcp-config`
- `clio-relay agent run`
- `clio-relay monitor add-regex`
- `clio-relay monitor run-once`

## http surfaces

The HTTP API exposes:

- health check
- job submission
- typed JARVIS submission
- typed remote-agent submission
- typed MCP-call submission
- job state
- job events
- task records
- stdout and stderr reads by offset
- artifact listing and reads
- progress reads
- cancellation

When `CLIO_RELAY_API_TOKEN` is set and the API is started with `--require-token`, clients must send either `Authorization: Bearer <token>` or `X-Clio-Relay-Token: <token>`. `/healthz` stays open for local process checks.

## mcp surfaces

The MCP server exposes relay tools for:

- submit JARVIS pipeline
- submit remote agent task
- submit remote MCP call
- monitor job
- watch event cursors
- list task records
- read logs
- list and read artifacts
- record and list progress
- create monitor rules
- cancel jobs

MCP tools operate on the same durable records as CLI and HTTP calls.

## environment and config

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

## live examples are not product defaults

The current live target uses Ares, Codex, Cloudflare-backed frp, and JARVIS builtin LAMMPS examples. Treat those as tested configurations, not fixed product semantics.
