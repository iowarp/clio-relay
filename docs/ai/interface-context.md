# Interface Context

This file is a dense map for coding agents. Keep the README shorter than this file.

## Connection model — read this before using any surface below

The surfaces in this file only make sense on top of one model. Misreading it has
already produced an ssh/scp-driven substitute system built around the relay,
which invalidated months of benchmark results. The normative statement is
[`docs/connection-model.md`](../connection-model.md). The short form:

**One connection is one local relay and one remote relay joined by exactly one
persistent link.** All control-plane and data-plane traffic rides that channel:
status, identity, submission, events, logs, artifact content, input ingest,
watch, cancellation, gateway operations, teardown. The link is established once
at bring-up and held. An operation never establishes transport; it uses the
transport that already exists.

**Three transport modes, one ssh budget.** (a) frp TCP through an
internet-accessible relay point that brokers two outbound connections — primary;
(b) frp XTCP UDP NAT-bypass to a direct peer link, falling back to (a); (c) one
held ssh port forward — fallback only. New ssh connections for the entire
lifetime of a connection: **at most one** in (a)/(b) (deploying the cluster
relay, skipped when already deployed), **at most two** in (c) (deploy plus the
held forward). After bring-up the count is **zero**, across any number of
operations, watches, recoveries, and staging transfers.

**The 2FA assumption is why.** A protected cluster requires interactive 2FA to
open an ssh connection. The user is present for the first connection and for
none after. Any path that could dial unattended is a violation even when it
succeeds — passwordless ssh on your test box is a property of that box, not a
licence to design around a prompt the real user will never see.

**One local relay manages many remote relays**, behind **one** virtual MCP
endpoint. Reaching a second cluster means naming it in the `cluster` argument,
never opening a second connection, tunnel, MCP registration, or shell.

**Run inputs are staged by the relay, never copied.** A file-typed package
setting declares `jarvis.configuration-input-binding.v1`; the caller passes a
path relative to `CLIO_RELAY_INPUT_WORKSPACE_ROOT` on the **client** machine;
the local door snapshots and hashes the bytes; they ingest through the owned
session over the one link; the cluster materializes a content-addressed copy;
relay rewrites the configuration to the staged reference; the job records
lineage. **`used_artifact_refs` non-empty, with a digest matching the client's
bytes, is the proof it engaged.** Empty on a declared file-typed setting means
the input arrived out of band: the run is not reproducible and is not evidence,
however successful it looks.

### Never do this

- Never `scp`/`rsync`/`sftp` a run input to the cluster. If it is an input, it
  goes through the staging contract or it does not go.
- Never pass a cluster-absolute path for a binding-declared setting. The value
  is a workspace-relative client path. A cluster path silently disables staging
  and empties `used_artifact_refs` while the job may still report success.
- Never re-dial ssh for status, polling, recovery, watch, cancellation, or
  staging. All of those are HTTP over the established link.
- Never treat relay as an ssh convenience wrapper, and never build a parallel
  ssh/scp path beside it. That substitutes an unrecorded system with no lineage,
  no provenance, no cancellation, and no reconnect, and anything measured
  through it describes the substitute rather than the product.
- Never host the client-facing door on the cluster and reach it through a
  tunnel. The door is local because the workspace it snapshots is the client's.
- Never assume the local relay can read cluster storage. It reads the client
  workspace and speaks HTTP to the remote relay; reading a cluster file means
  asking the remote side over the link, not `ssh <host> cat`.
- Never add a second local relay, MCP endpoint, or client config per cluster.

### Deviations you will see in the shipped code

State the design, not the deviation, in anything you write. These are tracked
defects, not the contract.

Two are closed by this release and survive only in older deployments:
per-operation ssh dialing in the owned-session control plane
([#179](https://github.com/iowarp/clio-relay/issues/179)), which now rides one
channel held per remote connection, and the built-in JARVIS door skipping input
staging ([#176](https://github.com/iowarp/clio-relay/issues/176)), which now
stages through the same plane as the registered route.

Still deviating: `relay_bind_jarvis_runtime` admission dialing four fresh ssh
per call; the `jarvis_service_runtime` scheduler bridge dialing ssh per
operation; cluster-targeted CLI dispatch opening an ssh connection per
invocation under `CLIO_RELAY_CLI_MODE=auto`; no production surface calling
`reconnect()` on a dropped channel; only the ssh-forward transport mode being
implemented; compute-node-side `frpc` carrying live service streams; and the
outstanding end-to-end staging proof
([#177](https://github.com/iowarp/clio-relay/issues/177)). `ControlMaster`
multiplexing and forward pooling are explicitly not fixes for any of them. The
full residual checklist is on
[#182](https://github.com/iowarp/clio-relay/issues/182); `docs/connection-model.md`
carries the same list in normative form.

## CLI Surfaces

Core setup:

- `clio-relay init`
- `clio-relay install-frp`
- `clio-relay cluster add`
- `clio-relay cluster bootstrap`
- `clio-relay cluster install-endpoint-service`
- `clio-relay cluster restart-endpoint-service`
- `clio-relay cluster endpoint-service-status`
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
- `clio-relay session plan-start`
- `clio-relay session start`
- `clio-relay session start-status`
- `clio-relay session start-watch`
- `clio-relay session status`
- `clio-relay session detach`
- `clio-relay session teardown`

Endpoint and job work:

- `clio-relay endpoint start`
- `clio-relay endpoint status`
- `clio-relay queue list`
- `clio-relay queue migration-status`
- `clio-relay queue migrate-indexes`
- `clio-relay queue audit-lease-capacity`
- `clio-relay queue repair-lease-indexes`
- `clio-relay queue diagnose <job-id>`
- `clio-relay queue stale`
- `clio-relay queue cleanup-stale`
- `clio-relay queue cancel <job-id>`
- `clio-relay queue validate <expendable-job-id>`
- `clio-relay queue retention-plan <job-id>`
- `clio-relay queue retention-status <job-id>`
- `clio-relay queue retention-collect <job-id>`
- `clio-relay scheduler status-batch` (internal bounded teardown query)
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
- `clio-relay job used-artifacts`
- `clio-relay job used-by`
- `clio-relay job read-artifact`
- `clio-relay job progress`
- `clio-relay gateway create`
- `clio-relay gateway list`
- `clio-relay gateway get`
- `clio-relay gateway update`
- `clio-relay gateway close`
- `clio-relay gateway start-runtime`
- `clio-relay gateway detach-runtime`
- `clio-relay gateway attach-runtime`
- `clio-relay gateway stop-runtime`
- `clio-relay gateway browser-attach` (internal trusted-viewer boundary)
- `clio-relay gateway browser-detach` (internal trusted-viewer boundary)

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
- content-pinned used-artifact and reverse used-by lineage reads
- authenticated exact-job transform record and nullable read
- owner-generation input-artifact ingest
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

`jarvis` and `remote_agent` HTTP submissions require the complete
`X-Clio-Relay-Owner-Session-Id` /
`X-Clio-Relay-Session-Generation-Id` attribution pair. Generic `mcp_call`
submissions accept the pair optionally and retain it when present. The pair is
recorded in explicit `RelayJob` fields and is not an admission credential.

Job POSTs to an owner-session-scoped API additionally require
`X-Clio-Relay-Owner-Session-Id` and
`X-Clio-Relay-Session-Generation-Id` for the exact live generation. The API
rejects missing, stale, or mismatched bindings and rejects client-supplied
ownership metadata; ownership is stamped only from the authenticated API process.
`POST /input-artifacts/ingest` is owner-generation-only and accepts the bounded,
hash-pinned payload used by schema-driven JARVIS input staging. `POST
/jobs/{job_id}/transform` records one immutable execution-owned activity and
`GET /jobs/{job_id}/transform` returns it or JSON `null`; both require normal API
authentication and exact job ownership, while POST also requires the session
submission binding.

`session plan-start`, `start`, `start-status`, and `start-watch` carry one exact
`clio-relay.owner-session-input-policy.v1` value derived from the validated
desktop configuration. It is part of selector identity and is persisted in the
cluster-local start journal and ready-session metadata. A different limit set
is a different owned-session identity and requires explicit replacement.

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
- query content-pinned artifact lineage in either direction
- record and list progress
- create, read, update, and close gateway sessions
- create monitor rules
- cancel jobs
- list the bounded relay queue
- diagnose one relay job with queue, lease, worker, scheduler, event, and progress evidence
- discover stale jobs without mutation
- clean stale jobs from the admin profile with dry-run and relay-only defaults
- bind connector-only gateways to authenticated, ready JARVIS service-runtime
  reports without accepting caller-supplied runtime or scheduler fields

MCP tools operate on the same durable records as CLI and HTTP calls.

`clio-relay mcp-server` uses native FastMCP over stdio or authenticated
Streamable HTTP. On MCP `2026-07-28`, it advertises
`io.modelcontextprotocol/tasks` and binds `tasks/get`, `tasks/update`, and
`tasks/cancel`. Standard task IDs equal relay job IDs. Virtual remote/JARVIS
operations may return tasks; low-level submit/status/read/cancel tools remain
immediate. The server does not run Docket. See `docs/mcp-tasks.md`.

`relay_artifact_lineage` is the single user-profile lineage query. Pass `job_id`
to list that job's immutable input edges or `artifact_id` to list downstream
consumer jobs; cluster routes use the normal `cluster` plus `route_revision`
pair. Submission tools accept `used_artifact_refs` as unique artifact-id/SHA-256
pairs with optional bounded `clio-relay.artifact-use-provenance.v1` evidence,
and owned-session routes enforce an exact producer/consumer session generation
match. Existing admin job/status reads include the nullable transform. No MCP
profile exposes transform mutation.

Registered remote MCP calls execute the packaged stdio client and server in
endpoint-owned process containment. They do not create an outer JARVIS pipeline
or scheduler job. Exact `clio-kit-jarvis-user-v3.7` routes additionally support
package-described local-file staging. Accepted bindings retain only
workspace-relative Host paths and immutable cluster artifact identities. A new
`jarvis_run` admission reconciles those paths into a checksum-bound input
manifest; the endpoint MCP runner materializes the manifest before the final
run call, while the same idempotency key reuses its original manifest without a
new snapshot. Other registered contracts remain generic pass-through.

`relay_bind_jarvis_runtime` is in the user profile. A waited
`jarvis_get_execution(include_service_runtimes=true)` response supplies compact
`service_runtime_bindings`; pass one exact entry unchanged as `binding`. It carries
the configured cluster, completed source job and `mcp_result` artifact, exact
package id/name, and service-instance id. Its fixed output reports `ready` or
`pending`; pending carries the exact same-gateway retry selector, no relay or
scheduler action, and six null URLs. Reissue the identical bind to resume it.
Ready includes the durable gateway session and six local URLs: connect, health,
stream, events, state, and command. The gateway stores the immutable source job/artifact digest,
execution and scheduler identities, service revision/report digest, and exact
dataset descriptor/digest. The tool does not accept submit, status, cancel, host,
port, path, descriptor overrides, or mixed compact/legacy selectors.

Normal bind and gateway-get results never contain a browser capability. A trusted
desktop viewer calls `gateway browser-attach` only after exact binding verification
and receives the one-time `clio-relay.browser-attachment.v1` six-URL contract. It
must call `gateway browser-detach` with the exact attachment id on viewer close.
Only safe `clio-relay.browser-attachment-record.v1` digest, expiry, process, and
revocation metadata remains in the gateway record. Browser requests require both
the URL capability and exact `Origin: null`; successful CORS is exactly `null`,
never `*`.

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
- `CLIO_RELAY_INPUT_WORKSPACE_ROOT`
- `CLIO_RELAY_INPUT_FILE_MAX_BYTES`
- `CLIO_RELAY_INPUT_TOTAL_MAX_BYTES`
- `CLIO_RELAY_INPUT_FILE_MAX_COUNT`

The three input-limit variables configure policy at the desktop planning
boundary. They are not ambient cluster defaults: the selected values are
serialized into the durable start contract and projected into the exact owned
API generation.

`CLIO_RELAY_INPUT_WORKSPACE_ROOT` names a directory on the **client** machine —
the only place the local door is allowed to read run inputs from. It is never a
cluster path. `CLIO_RELAY_CLI_MODE` selects how a cluster-targeted CLI command
is executed: `local` keeps it in-process, and `auto`/`ssh` currently open an ssh
connection for that invocation, which is the tracked deviation noted above.

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
