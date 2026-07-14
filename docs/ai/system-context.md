# System Context

`clio-relay` is a relay layer for submitting and observing remote work. It is part of the `clio-agent` federation story, but it must remain usable by non-CLIO clients through CLI, HTTP, and MCP.

## Core Boundaries

- `clio-core` is the authoritative queue and state boundary.
- The file-backed queue in this repository is a development backend for the same record contract.
- frp and SSH forwarding are byte transports only.
- JARVIS-CD owns cluster execution, scheduler integration, package behavior, output collection, and provenance.
- Application-specific behavior belongs in JARVIS packages or package-aware adapters, not in generic relay core code.

## Roles

- `desktop`: accepts local user, agent, HTTP, and MCP requests and submits durable jobs.
- `worker`: runs on a configured cluster, leases jobs, invokes JARVIS-CD, streams events, and writes artifacts.
- `relay-host`: renders or runs frp host configuration. It must not own queue state or job state.

## Records

The durable record families are:

- endpoints
- jobs
- tasks
- leases
- events
- cursors
- artifacts
- progress
- checkpoints
- idempotency records

Each job has monotonic events and cursor-based replay. Logs are readable by byte offset. Artifacts are indexed records that can point to backing files.

## Execution Semantics

Job submission is asynchronous by default. Submit returns a `job_id`, state, kind, and terminal flag. Clients can choose synchronous waiting only when they are not blocking the only worker that can execute child work.

Remote agent jobs should usually submit child cluster work asynchronously and return the child `job_id`. A cluster worker running a parent agent job cannot also execute a child job if it is waiting synchronously inside the parent.

Cancellation is durable and cooperative. A cancel request records queue events and the worker terminates the running process group. For scheduler-backed packages, package code should capture scheduler job ids and cancel through the scheduler when needed.
Session teardown quiesces the exact owner-session generation before discovery. With
`--cancel-jobs`, it waits boundedly for worker cleanup acknowledgment of leased and
running jobs before gateway or API cleanup. It stops the API to seal intake, rescans
for pre-quiescence in-flight submissions, and acknowledges those before gateway
cleanup. A timeout leaves intake quiesced and the remaining resources explicit.

JARVIS-owned execution is authoritative only when it supplies the exact `jarvis.execution.handle.v1`, `jarvis.execution.record.v1`, and `jarvis.execution.progress.v1` documents. The relay preserves those documents and projects them into `clio-relay.jarvis-runtime.v1` for job/task metadata, events, artifacts, and provenance. The older `jarvis.runtime.v1`, flexible structured payloads, and stdout scheduler patterns are compatibility evidence only and cannot authorize polling or cancellation or satisfy the 1.0 gate.

## Progress

JARVIS core owns execution IDs, durable `jarvis.progress.v1` events, and aggregate execution progress. Package-local code owns only application-specific interpretation. Native MCP notifications carry exact execution progress snapshots and remain distinct from MCP transport sequence numbers.

`clio_relay.package_progress_adapters` and generic regex progress remain explicitly labeled compatibility paths. They cannot satisfy a 1.0 release claim, even when their provider entry point or parsed output is otherwise valid.

ETA is based on observed iterative progress after warmup. It should include confidence or sample metadata when exposed to clients.

## Transports

Supported paths:

- frp STCP over WebSocket/TLS for Cloudflare-backed or HTTPS-edge relay hosts.
- frp STCP over TCP for public or institutional relay hosts.
- SSH local port forwarding for closed environments that already have SSH or VPN access.
- frp XTCP probing as an optional optimization with fallback.

Transport failure must not corrupt queue state. Direct transport and NAT punching are optimizations, not reliability requirements.

## Sessions

Remote sessions are owned by a session id. Session metadata and PID files live under `$HOME/.local/share/clio-relay/sessions/<session-id>`.

The desktop shutdown choices are separate:

- close local UI, stop owned desktop connectors, and detach from the remote session.
- close local UI, stop verified owned local/remote connectors and relay API processes,
  and close gateway records owned by the session.
- close local UI and also stop the persistent worker service.

Only the last option should call `session teardown --stop-worker`.
Relay jobs are retained unless `--cancel-jobs` is explicit. Scheduler work is
retained unless `--cancel-scheduler-jobs` is also explicit. Cleanup reports must
include ownership verification, action, outcome, and residual-resource fields.
Teardown persists one immutable cleanup operation and policy before side effects;
same-policy retries are idempotent, policy drift is rejected, and attach/detach is
forbidden after that intent exists. Connector evidence must map one desktop and
one remote disposition to each owned gateway. A natural scheduler terminal state
during a cancel race permits safe closure but must not be reported as canceled.
An owner-session gateway without the exact active generation is ambiguous: detach
or teardown reports it as a residual resource and does not use the session id alone
to authorize connector or gateway cleanup.

## Scheduler provider boundary

Every cluster has an explicit `scheduler_provider`. `external` is the generic
default for package-owned or nonscheduler runtimes; a SLURM site selects `slurm`
in its cluster definition. Missing provider metadata must never silently become
SLURM. Structured JARVIS runtime metadata is preferred for provider and job ids,
and must agree with the worker's configured provider before polling or canceling.

## Configurability

Do not hardcode:

- `ares`
- `codex`
- `frps.jcernuda.com`
- application names, package log formats, or installer commands
- Cloudflare
- local filesystem roots

Those are configured examples or live acceptance targets. The implementation must support a second configured machine such as `homelab` without changing core semantics.
